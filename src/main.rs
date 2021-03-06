#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]
#![feature(question_mark)]

#[macro_use]
extern crate log;
extern crate serde;
extern crate serde_json;
extern crate hyper;
extern crate env_logger;
extern crate sha1;
extern crate data_encoding;
extern crate crossbeam;

use serde_json::Value;
use hyper::Client;
use hyper::header::Connection;
use std::io::Read;
use std::error::Error;
use std::fmt::Write as WriteFmt;
use std::sync::mpsc::{SyncSender,Receiver};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::time::Duration;
use crossbeam::scope;


struct Config {
    source_host: String,
    source_port: u16,
    source_index: String,
    source_shards: Vec<u32>,
    target_host: String,
    target_port: u16,
    target_index: String,
    scroll_timeout: String,
    bulk_size: usize,
    queue_len: usize,
    nb_sender_threads : usize,
    request: String,
}

#[derive(Serialize)]
struct Action<'a> {
    _index: &'a str,
    _type: &'a str,
    _id: &'a str,
    _version: u64,
    _version_type: &'a str
}

impl Config {
    fn new() -> Config {
        let mut args = std::env::args();
        if args.len() != 13 {
            error!("usage: es-reindex source_host source_port source_index source_shards(comma separated) target_host target_port target_index scroll_timeout bulk_size queue_len nb_sender_threads request");
            panic!("incorrect number of arguments: {} expected 13", args.len());
        }

        let _ = args.next();
        Config {
            source_host :       args.next().unwrap(),
            source_port :       args.next().unwrap().parse().expect("source_port is not a number"),
            source_index :      args.next().unwrap(),
            source_shards :     args.next().unwrap().split(',').map(|s| s.parse().expect("source_shard is not a number")).collect(),
            target_host :       args.next().unwrap(),
            target_port :       args.next().unwrap().parse().expect("target_port is not a number"),
            target_index :      args.next().unwrap(),
            scroll_timeout :    args.next().unwrap(),
            bulk_size :         args.next().unwrap().parse().expect("bulk_size is not a number"),
            queue_len :         args.next().unwrap().parse().expect("queue_len is not a number"),
            nb_sender_threads : args.next().unwrap().parse().expect("nb_sender_threads is not a number"),
            request:            args.next().unwrap(),
        }
    }
}


fn es_scan_and_scroll_thread(cfg: Arc<Config>, shard: u32, channels: Vec<SyncSender<Vec<String>>>) {
    let client = Client::new();
    let mut buf = Vec::new();
    let mut m = sha1::Sha1::new();
    let mut digest = [0u8; 20];
    let mut fannout = 0;
    let mut counter = 0u8;

    // get ES scroll id
    let url = format!("http://{}:{}/{}/_search?scroll={}&search_type=scan&preference=_shards:{};_local&size={}&version", cfg.source_host, cfg.source_port, cfg.source_index, cfg.scroll_timeout, shard, cfg.bulk_size);
    let mut http_conn;

    // loop until the http request succeed
    loop {
        debug!(target: "scan_and_scroll", "shard {: >3} - requesting scroll_id", shard);
        match client.post(&url).header(Connection::keep_alive()).body(&cfg.request).send() {
            Ok(h) => {
                http_conn = h;
                break;
            },
            Err(e) => {
                warn!(target: "scan_and_scroll", "shard {: >3} - http error: {:?}", shard, e);
                debug!(target: "scan_and_scroll", "shard {: >3} - sleeping 5 seconds", shard);
                thread::sleep(Duration::from_secs(5));
            }
        }
    }

    buf.clear();
    http_conn.read_to_end(&mut buf).map_err(|e| e.description().to_string()).unwrap();
    let data: Value = match serde_json::from_str(&String::from_utf8_lossy(&buf)) {
        Ok(d) => d,
        Err(e) => {
            error!(target: "scan_and_scroll", "shard {: >3} - invalid json from scroll request response: {:?}\n\t{:?}", shard, e, String::from_utf8_lossy(&buf));
            return;
        }
    };
    let scroll_id_v = data.as_object().unwrap().get("_scroll_id").unwrap();
    let mut scroll_id = match *scroll_id_v {
        Value::String(ref s) => s.clone(),
        _ => panic!("_scroll_id is not a string")
    };
    info!(target: "scan_and_scroll", "shard {: >3} - got scroll_id: {}", shard, scroll_id);

    // scroll data
    let url = format!("http://{}:{}/_search/scroll?scroll={}", cfg.source_host, cfg.source_port, cfg.scroll_timeout);
    loop {
        // download data
        let mut http_conn;
        // loop until the http request succeed
        loop {
            debug!(target: "scan_and_scroll", "shard {: >3} - requesting scan&scroll", shard);
            match client.post(&url).header(Connection::keep_alive()).body(&scroll_id).send() {
                Ok(h) => {
                    http_conn = h;
                    break;
                },
                Err(e) => {
                    warn!(target: "scan_and_scroll", "shard {: >3} - http error: {:?}", shard, e);
                    debug!(target: "scan_and_scroll", "shard {: >3} - sleeping 5 seconds", shard);
                    thread::sleep(Duration::from_secs(5));
                }
            }
        }

        debug!(target: "scan_and_scroll", "shard {: >3} - downloading scan&scroll", shard);
        buf.clear();
        http_conn.read_to_end(&mut buf).map_err(|e| e.description().to_string()).unwrap();
        let data: Value = match serde_json::from_str(&String::from_utf8_lossy(&buf)) {
            Ok(d) => d,
            Err(e) => {
                error!(target: "scan_and_scroll", "shard {: >3} - invalid json from scroll request response: {:?}\n\t{:?}", shard, e, String::from_utf8_lossy(&buf));
                return;
            }
        };

        // periodically shrink buffer to avoid wasting memory
        if counter == 100 {
            buf.shrink_to_fit();
            counter = 0;
        } else {
            counter += 1;
        }

        // extract new scroll id
        let scroll_id_v = data.as_object().unwrap().get("_scroll_id").unwrap();
        scroll_id.clear();
        scroll_id.push_str(match *scroll_id_v {
            Value::String(ref s) => s,
            _ => panic!("_scroll_id is not a string")
        });

        // extract data
        let json = data.as_object().expect("json root is not an object");

        let hits = json.get("hits").expect("no field hits").as_object().expect("hits is not an object")
                       .get("hits").expect("no field hits").as_array().expect("hits is not an array");

        let shards = json.get("_shards").expect("no field _shards").as_object().expect("_shards is not an object");
        if let Some(failures) = shards.get("failures") {
            warn!(target: "scan_and_scroll", "shard {: >3} - failures: {:?}", shard, failures);
            COUNTER_FAILED_SHARDS.fetch_add(1, Ordering::SeqCst);
            break;
        }

        // if no more data, exit loop
        if hits.len() == 0 {
            info!(target: "scan_and_scroll", "shard {: >3} - scan and scroll thread finished", shard);
            break;
        }

        // construct action list
        let mut actions = Vec::<String>::new();
        for doc in hits {
            let doc = doc.as_object().expect("doc not an object");

            let version = doc.get("_version").expect("_version not found").as_u64().expect("_version is not a number");
            let type_   = doc.get("_type").expect("_type not found").as_string().expect("_type is not an string");
            //let id      = doc.get("_id").expect("_id not found").as_string().expect("_id is not an string");
            let source  = doc.get("_source").expect("_source not found").as_object().expect("_source is not an object");

            /* BEGIN CAPITALISATION SPECIFIC CODE */

            let doc_login  = source.get("login").expect("login not found").as_string().expect("login is not a string");
            let doc_log  = source.get("log").expect("log not found").as_string().expect("log is not a string");
            let doc_alias  = source.get("alias").map(|i| i.as_string().expect("alias is not a string"));
            let doc_login_id  = source.get("loginId").map(|i| i.as_string().expect("loginId is not a string"));
            let doc_source = source.get("source").expect("source not found").as_string().expect("source is not a string");
            //let doc_date = source.get("date").expect("date not found").as_string().expect("date is not a string");
            let doc_restriction_list = source.get("restrictionList").expect("restrictionList not found").as_string().expect("restrictionList is not a string");

            m.reset();
            if let Some(alias) = doc_alias {
                m.update(alias.as_bytes());
            };
            m.update(doc_log.as_bytes());
            m.update(doc_login.as_bytes());
            if let Some(login_id) = doc_login_id {
                m.update(login_id.as_bytes());
            };
            m.update(doc_restriction_list.as_bytes());
            m.update(doc_source.as_bytes());
            m.output(&mut digest);

            let mut doc_id = String::new();
            doc_id.push_str(&data_encoding::base64::encode(&digest));

            let id = &doc_id;
            /* END CAPITALISATION SPECIFIC CODE */

            let mut action_string = String::new();

            let action = Action {
                _index: &cfg.target_index,
                _type: type_,
                _id: id,
                _version: version,
                _version_type: "external"
            };
            WriteFmt::write_fmt(&mut action_string,
                format_args!("{{\"index\":{}}}\n{}\n",
                    serde_json::to_string(&action).unwrap(),
                     &serde_json::to_string(&source).unwrap()))
            .unwrap();

            actions.push(action_string);
        }

        // send action list
        debug!(target: "scan_and_scroll", "shard {: >3} - sending scan&scroll to channel", shard);
        channels[fannout].send(actions).unwrap();
        fannout += 1;
        if fannout >= channels.len() {
            fannout = 0;
        }
    } // end loop

}


fn es_bulk_index_thread(cfg: Arc<Config>, chan: Receiver<Vec<String>>, shard: u32, thread: usize) {
    let client = Client::new();
    let mut buf = Vec::new();
    let mut counter = 0u8;

    // loop on bulk stream
    let url = format!("http://{}:{}/_bulk", cfg.target_host, cfg.target_port);
    loop {
        // receive action list
        debug!(target: "bulk_index", "shard {: >3}, thread {: >2} - receiving bulk from channel", shard, thread);
        let actions = match chan.recv() {
            Ok(actions) => actions,
            Err(_)      => {
                info!(target: "bulk_index", "shard {: >3}, thread {: >2} - bulk indexation thread finished", shard, thread);
                return;
            }
        };

        // loop on bulk execution attempts
        let mut to_insert_docs = vec![true;actions.len()];
        loop {

            // construct bulk request
            let mut bulk_req = String::new();
            let mut index = 0;
            for action in actions.iter() {
                if to_insert_docs[index] {
                    bulk_req.push_str(&action);
                }
                index += 1;
            }

            // send bulk request
            let mut http_conn;
            // loop until the http request succeed
            loop {
                debug!(target: "bulk_index", "shard {: >3}, thread {: >2} - requesting scan&scroll", shard, thread);
                match client.post(&url).header(Connection::keep_alive()).body(&bulk_req).send() {
                    Ok(h) => {
                        http_conn = h;
                        break;
                    },
                    Err(e) => {
                        warn!(target: "bulk_index", "shard {: >3}, thread {: >2} - http error: {:?}", shard, thread, e);
                        debug!(target: "bulk_index", "shard {: >3}, thread {: >2} - sleeping 5 seconds", shard, thread);
                        thread::sleep(Duration::from_secs(5));
                    }
                }
            }

            // parse response
            debug!(target: "bulk_index", "shard {: >3}, thread {: >2} - receiving bulk request response", shard, thread);
            buf.clear();
            http_conn.read_to_end(&mut buf).map_err(|e| e.description().to_string()).unwrap();
            let data: Value = match serde_json::from_str(&String::from_utf8_lossy(&buf)) {
                Ok(d) => d,
                Err(e) => {
                    error!(target: "bulk_index", "shard {: >3}, thread {: >2} - invalid json from bulk request response: {:?}\n\t{:?}", shard, thread, e, String::from_utf8_lossy(&buf));
                    continue;
                }
            };

            // periodically shrink buffer to avoid wasting memory
            if counter == 100 {
                buf.shrink_to_fit();
                counter = 0;
            } else {
                counter += 1;
            }

            // check for errors
            if let Some(err) = data.as_object().expect("json root is not an object").get("errors") {
                if !err.as_boolean().expect("errors is not a boolean"){
                    // if no errors break early
                    break;
                }
            }

            // there is some errors, check them

            // parse error items
            if let Some(items_value) = data.as_object().expect("json root is not an object").get("items") {
                let items = items_value.as_array().expect("items is not an array");
            

                // loop on error items
                let mut index = 0;
                let mut to_retry = false;
                for item in items {
                    // get status
                    let item = item.as_object().expect("item not an object").get("index").expect("no field index").as_object().expect("index is not an object");
                    let status = item.get("status").expect("status not found").as_u64().expect("status is not a number");

                    if status == 200 || status == 201 || status == 409 { // no error
                        to_insert_docs[index] = false;
                    } else if status == 400 { // real error with document
                        // don't try to reindex it but log the error
                        to_insert_docs[index] = false;
                        if let Some(error) = item.get("error") {
                            warn!(target: "bulk_index", "shard {: >3}, thread {: >2} - error {} on document, skiping\n\t{:?}\n\t{}", shard, thread, status, error, actions[index]);
                        } else {
                            warn!(target: "bulk_index", "shard {: >3}, thread {: >2} - error {} on document, skiping\n\t{}", shard, thread, status, actions[index]);
                        }
                    } else { // probably an error with ES, retry injecting
                        to_retry = true;
                        if let Some(error) = item.get("error") {
                            debug!(target: "bulk_index", "shard {: >3}, thread {: >2} - error {} on action\n\t{:?}\n\t{}", shard, thread, status, error, actions[index]);
                        } else {
                            debug!(target: "bulk_index", "shard {: >3}, thread {: >2} - error {} on action\n\t{}", shard, thread, status, actions[index]);
                        }
                    }

                    index += 1;
                }

                // if nothing to retry, break loop
                if !to_retry {
                    break;
                }
            } else {
                // WTF is this response!, retry everything!
                error!(target: "bulk_index", "shard {: >3}, thread {: >2} - WTF error {:?}", shard, thread, data);
            } 
        }

        let count = COUNTER_INDEXED_DOC.fetch_add(actions.len(), Ordering::SeqCst);
        let failed_shards = COUNTER_FAILED_SHARDS.load(Ordering::SeqCst);
        info!(target: "bulk_index", "shard {: >3}, thread {: >2} - bulk request successful. indexed docs: {} - failed shards: {}", shard, thread, count + actions.len(), failed_shards);
    }
}

fn reindex_shard(cfg: Arc<Config>, shard: u32) {
    let mut senders   = Vec::new();
    let mut receivers = Vec::new();

    for _ in 0..cfg.nb_sender_threads {
        let (sender, receiver) = sync_channel(cfg.queue_len);
        senders.push(sender);
        receivers.push(receiver);
    }

    crossbeam::scope(|scope| {
        let cfg_scan = cfg.clone();
        scope.spawn(move || {
            es_scan_and_scroll_thread(cfg_scan, shard, senders);
        });

        for (i, receiver) in receivers.drain(0..).enumerate() {
            let cfg_bulk = cfg.clone();
            scope.spawn(move || {
                es_bulk_index_thread(cfg_bulk, receiver, shard, i);
            });
        }
    });
}

static COUNTER_INDEXED_DOC: AtomicUsize = ATOMIC_USIZE_INIT;
static COUNTER_FAILED_SHARDS: AtomicUsize = ATOMIC_USIZE_INIT;

fn main() {
    env_logger::init().unwrap();
    let cfg = Arc::new(Config::new());

    crossbeam::scope(|scope| {
        for i in cfg.source_shards.clone().into_iter() {
            let cfg_reindex = cfg.clone();
            scope.spawn(move || {
                reindex_shard(cfg_reindex, i);
            });
        }
    });

    info!("main thread finished");

    return;
}
