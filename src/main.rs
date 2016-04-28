#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]
#![feature(question_mark)]
#![feature(start)]

extern crate serde;
extern crate serde_json;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate sha1;
extern crate data_encoding;

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


struct Config {
    source_host: String,
    source_port: u16,
    source_index: String,
    source_shards: String,
    target_host: String,
    target_port: u16,
    target_index: String,
    scroll_timeout: String,
    bulk_size: usize,
    queue_len: usize
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
        let _ = args.next();
        Config {
            source_host :       args.next().unwrap(),
            source_port :       args.next().unwrap().parse::<u16>().unwrap(),
            source_index :      args.next().unwrap(),
            source_shards :     args.next().unwrap(),
            target_host :       args.next().unwrap(),
            target_port :       args.next().unwrap().parse::<u16>().unwrap(),
            target_index :      args.next().unwrap(),
            scroll_timeout :    args.next().unwrap(),
            bulk_size :         args.next().unwrap().parse::<usize>().unwrap(),
            queue_len :         args.next().unwrap().parse::<usize>().unwrap(),
        }
    }
}


fn es_scan_and_scroll_thread(cfg: Arc<Config>, chan: SyncSender<Vec<String>>) {
    let client = Client::new();
    let mut buf = Vec::new();
    let mut m = sha1::Sha1::new();
    let mut digest = [0u8; 20];


    // get ES scroll id
    debug!("requesting scroll_id");
    let url = format!("http://{}:{}/{}/_search?scroll={}&search_type=scan&preference=_shards:{};_local&size={}&version", cfg.source_host, cfg.source_port, cfg.source_index, cfg.scroll_timeout, cfg.source_shards, cfg.bulk_size);
    let mut http_conn = client.post(&url).header(Connection::keep_alive()).send().unwrap();
    http_conn.read_to_end(&mut buf).map_err(|e| e.description().to_string()).unwrap();
    let data: Value = serde_json::from_str(&String::from_utf8_lossy(&buf)).unwrap();
    buf.clear();
    let scroll_id_v = data.as_object().unwrap().get("_scroll_id").unwrap();
    let mut scroll_id = match *scroll_id_v {
        Value::String(ref s) => s.clone(),
        _ => panic!("_scroll_id is not a string")
    };
    info!("got scroll_id: {}", scroll_id);


    // scroll data
    let url = format!("http://{}:{}/_search/scroll?scroll={}", cfg.source_host, cfg.source_port, cfg.scroll_timeout);
    loop {
        // download data
        debug!("requesting scan&scroll");
        let mut http_conn = client.post(&url).header(Connection::keep_alive()).body(&scroll_id).send().unwrap();
        debug!("downloading scan&scroll");
        http_conn.read_to_end(&mut buf).map_err(|e| e.description().to_string()).unwrap();
        debug!("processing scan&scroll");
        let data: Value = {
            let buf_utf8 = &String::from_utf8_lossy(&buf);
            trace!("{:?}", buf_utf8);
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
        let hits = data.as_object().ok_or("json root is not an object").unwrap()
                        .get("hits").ok_or("no field hits").unwrap().as_object().ok_or("hits is not an object").unwrap()
                        .get("hits").ok_or("no field hits").unwrap().as_array().ok_or("hits is not an array").unwrap();

        // if no more data, exit loop
        if hits.len() == 0 {
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
            trace!("{:?}", source);
            WriteFmt::write_fmt(&mut action_string,
                format_args!("{{\"index\":{}}}\n{}\n", 
                    serde_json::to_string(&action).unwrap(),
                     &serde_json::to_string(&source).unwrap()))
            .unwrap();

            actions.push(action_string);
        }

        // send action list
        debug!("sending scan&scroll to channel");
        chan.send(actions).unwrap();
    } // end loop
    
    info!("scan and scroll thread finished");

}


fn es_bulk_index_thread(cfg: Arc<Config>, chan: Receiver<Vec<String>>) {
    let client = Client::new();
    let mut body = String::new();
    let mut counter_indexed_docs = 0u64;

    // loop on bulk stream
    let url = format!("http://{}:{}/_bulk", cfg.target_host, cfg.target_port);
    loop {
        // receive action list
        debug!("receiving bulk from channel");
        let actions = match chan.recv() {
            Ok(actions) => actions,
            Err(_)      => {
                info!("bulk indexation thread finished");
                return;
            }
        };

        counter_indexed_docs += actions.len() as u64;

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
            debug!("sending bulk request");
            trace!("{}",bulk_req);
            let mut http_conn = client.post(&url).header(Connection::keep_alive()).body(&bulk_req).send().unwrap();
            
            // parse response
            debug!("receiving bulk request response");
            http_conn.read_to_string(&mut body).map_err(|e| e.description().to_string()).unwrap();
            let data: Value = serde_json::from_str(&body).unwrap();
            body.clear();

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
                    let error = item.get("error").ok_or("error not found").unwrap().as_string().ok_or("error is not a string").unwrap();
                    debug!("error {} on action\n\t{}\n\t{}", status, error, actions[index]);
                }

                index += 1;
            }

            // if nothing to retry, break loop
            if !to_retry {
                break;
            }
        }
        debug!("bulk request successful. indexed docs: {}", counter_indexed_docs);
    }   
}

#[derive(Serialize, Deserialize)]
struct Person {
    name: String
}
use std::char;

fn main() {
/*
    let c = match 0xDC51 { // FIRST BYTE
        0xDC00 ... 0xDFFF => {
            panic!("LoneLeadingSurrogateInHexEscape");
        }

        // Non-BMP characters are encoded as a sequence of
        // two hex escapes, representing UTF-16 surrogates.
        n1 @ 0xD800 ... 0xDBFF => {
            println!("NON BMP");
            match (Some(b'\\'), Some(b'u')) {
                (Some(b'\\'), Some(b'u')) => (),
                _ => {
                    panic!("UnexpectedEndOfHexEscape");
                }
            }

            let n2 = 0xDC51; // SECOND BYTE

            if n2 < 0xDC00 || n2 > 0xDFFF {
                panic!("LoneLeadingSurrogateInHexEscape");
            }

            let n = (((n1 - 0xD800) as u32) << 10 |
                      (n2 - 0xDC00) as u32) + 0x1_0000;

            match char::from_u32(n as u32) {
                Some(c) => c,
                None => {
                    panic!("InvalidUnicodeCodePoint");
                }
            }
        }

        n => {
            match char::from_u32(n as u32) {
                Some(c) => c,
                None => {
                    panic!("InvalidUnicodeCodePoint");
                }
            }
        }
    };

    // FIXME: this allocation is required in order to be compatible with stable
    // rust, which doesn't support encoding a `char` into a stack buffer.
    println!("{:?}", c);

    return; */
    /*
    //let data: Value = serde_json::from_str(r#"{"foo": "1:῿ 2:🿿 3: 4:👑 5:ÿ"}"#).unwrap();
    let data: Value = serde_json::from_str(r#"{"foo": "1:῿ 2:🿿 3: 4:👑 5:ÿ 6:\uD83D\uDC51\uDC51END \\u06Jfsdhjk"}"#).unwrap();
    let foo = data.as_object().ok_or("item not an object").unwrap().get("foo").ok_or("foo not found").unwrap().as_string().ok_or("foo is not a string").unwrap();
    println!("{:?}", foo);
    println!("{}", foo);
    println!("{}",serde_json::to_string(&foo).unwrap());

    return; 
    
    let a=Person{name: r#"foo": "1:῿ 2:🿿 3: 4:👑 5:ÿ "#.to_string()};
    println!("{}",serde_json::to_string(&a).unwrap());
    return;
    */
    env_logger::init().unwrap();

    let args = std::env::args();
    if args.len() != 11 {
        error!("usage: es-reindex source_host source_port source_index source_shard target_host target_port target_index scroll_timeout bulk_size queue_len");
        error!("incorrect number of arguments: {} expected 11", args.len());
        return;
    }

    let cfg = Arc::new(Config::new());

    let (sender, receiver) = sync_channel(cfg.queue_len);
    
    let cfg_scan = cfg.clone();
    let scan_thread = thread::spawn(move || {
        es_scan_and_scroll_thread(cfg_scan, sender);
    });

    let cfg_bulk = cfg.clone();
    let bulk_thread = thread::spawn(move || {
        es_bulk_index_thread(cfg_bulk, receiver);
    });

    scan_thread.join().unwrap();
    bulk_thread.join().unwrap();
    
    info!("main thread finished");

    return;

    /*
    let json = b"{\"foo\": 13, \"bar\": \"\x03baz\xe2\x88\"}";
    let json_str = String::from_utf8_lossy(json);
    let data: Value = serde_json::from_str(&json_str).unwrap();
    println!("data: {:?}", data);
    // data: {"bar":"baz","foo":13}
    println!("object? {}", data.is_object());
    // object? true

    let obj = data.as_object().unwrap();
    let foo = obj.get("foo").unwrap();

    println!("array? {:?}", foo.as_array());
    // array? None
    println!("u64? {:?}", foo.as_u64());
    // u64? Some(13u64)

    for (key, value) in obj.iter() {
        println!("{}: {}", key, match *value {
            Value::U64(v) => format!("{} (u64)", v),
            Value::String(ref v) => format!("{} (string)", v),
            _ => format!("other")
        });
    }
    */
    // bar: baz (string)
    // foo: 13 (u64)
}


/*

FIX Inhano truncate
Validate rust truncate

allow reindex from provided scroll_id
allow reindex from multiple shards in parallele

better handle bulk error logging




*/