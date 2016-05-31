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
            source_port :       args.next().unwrap().parse::<u16>().expect("source_port is not a number"),
            source_index :      args.next().unwrap(),
            source_shards :     args.next().unwrap().split(',').map(|s| s.parse().expect("source_shard is not a number")).collect(),
            target_host :       args.next().unwrap(),
            target_port :       args.next().unwrap().parse::<u16>().expect("target_port is not a number"),
            target_index :      args.next().unwrap(),
            scroll_timeout :    args.next().unwrap(),
            bulk_size :         args.next().unwrap().parse::<usize>().expect("bulk_size is not a number"),
            queue_len :         args.next().unwrap().parse::<usize>().expect("queue_len is not a number"),
            nb_sender_threads : args.next().unwrap().parse::<usize>().expect("nb_sender_threads is not a number"),
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

    http_conn.read_to_end(&mut buf).map_err(|e| e.description().to_string()).unwrap();
    let data: Value = serde_json::from_str(&String::from_utf8_lossy(&buf)).unwrap();
    buf.clear();
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
        http_conn.read_to_end(&mut buf).map_err(|e| e.description().to_string()).unwrap();
        debug!(target: "scan_and_scroll", "shard {: >3} - processing scan&scroll", shard);
        let data: Value = {
            let buf_utf8 = &String::from_utf8_lossy(&buf);
            serde_json::from_str(buf_utf8).unwrap()
        };
        buf.clear();

        // extract new scroll id
        let scroll_id_v = data.as_object().unwrap().get("_scroll_id").unwrap();
        scroll_id.clear();
        scroll_id.push_str(match *scroll_id_v {
            Value::String(ref s) => s,
            _ => panic!("_scroll_id is not a string")
        });

        // extract data
        let json = data.as_object().ok_or("json root is not an object").unwrap();

        let hits = json.get("hits").ok_or("no field hits").unwrap().as_object().ok_or("hits is not an object").unwrap()
                       .get("hits").ok_or("no field hits").unwrap().as_array().ok_or("hits is not an array").unwrap();

        let shards = json.get("_shards").ok_or("no field _shards").unwrap().as_object().ok_or("_shards is not an object").unwrap();
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
            let doc = doc.as_object().ok_or("doc not an object").unwrap();

            let version = doc.get("_version").ok_or("_version not found").unwrap().as_u64().ok_or("_version is not a number").unwrap();
            let type_   = doc.get("_type").ok_or("_type not found").unwrap().as_string().ok_or("_type is not an string").unwrap();
            //let id      = doc.get("_id").ok_or("_id not found").unwrap().as_string().ok_or("_id is not an string").unwrap();
            let source  = doc.get("_source").ok_or("_source not found").unwrap().as_object().ok_or("_source is not an object").unwrap();

            /* BEGIN CAPITALISATION SPECIFIC CODE */

            let doc_login  = source.get("login").ok_or("login not found").unwrap().as_string().ok_or("login is not a string").unwrap();
            let doc_log  = source.get("log").ok_or("log not found").unwrap().as_string().ok_or("log is not a string").unwrap();
            let doc_alias  = source.get("alias").map(|i| i.as_string().ok_or("alias is not a string").unwrap());
            let doc_login_id  = source.get("loginId").map(|i| i.as_string().ok_or("loginId is not a string").unwrap());
            let doc_source = source.get("source").ok_or("source not found").unwrap().as_string().ok_or("source is not a string").unwrap();
            //let doc_date = source.get("date").ok_or("date not found").unwrap().as_string().ok_or("date is not a string").unwrap();
            let doc_restriction_list = source.get("restrictionList").ok_or("restrictionList not found").unwrap().as_string().ok_or("restrictionList is not a string").unwrap();

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
    let mut body = String::new();

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
            body.clear();
            http_conn.read_to_string(&mut body).map_err(|e| e.description().to_string()).unwrap();
            let data: Value = match serde_json::from_str(&body) {
                Ok(d) => d,
                Err(e) => {
                    error!(target: "bulk_index", "shard {: >3}, thread {: >2} - invalid json from bulk request response: {:?}\n\t{:?}", shard, thread, e, body);
                    continue;
                }
            };

            // check for errors
            let is_errors = data.as_object().ok_or("json root is not an object").unwrap()
                            .get("errors").ok_or("no field errors").unwrap().as_boolean().ok_or("errors is not a boolean").unwrap();
            if !is_errors {
                // if no errors break early
                break;
            }
            // there is some errors, check them

            // parse error items
            let items = data.as_object().ok_or("json root is not an object").unwrap()
                            .get("items").ok_or("no field items").unwrap().as_array().ok_or("items is not an array").unwrap();

            // loop on error items
            let mut index = 0;
            let mut to_retry = false;
            for item in items {
                // get status
                let item = item.as_object().ok_or("item not an object").unwrap().get("index").ok_or("no field index").unwrap().as_object().ok_or("index is not an object").unwrap();
                let status = item.get("status").ok_or("status not found").unwrap().as_u64().ok_or("status is not a number").unwrap();

                if status == 200 || status == 201 || status == 409 { // no error
                    to_insert_docs[index] = false;
                } else {
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
