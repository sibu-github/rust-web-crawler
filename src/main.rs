use std::collections::HashSet;
use mongodb::Client;
use mongodb::bson::doc;
use mongodb::bson::Document;
use mongodb::bson::oid::ObjectId;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use tokio::task;
use tokio::time::{sleep, Duration};
use chrono::Utc;
use regex::Regex;
use sha2::Sha256;
use sha2::Digest;
use sha2::digest::Output;

const DB_URL:&str = "mongodb+srv://dbusrbangla:***********@bangla-word.rwht8.mongodb.net/banglaWord";
const DB_NAME: &str = "banglaWord";
const WORD_COLL: &str = "allWords";
const URL_COLL: &str = "allUrl";
const WORKER_SLEEP_DURATION: u64 = 1000;

#[derive(Debug, Serialize, Deserialize)]
struct AllUrl {
    _id: ObjectId,
    url: String,
    visited: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct AllWord {
    val: String,
    hash: String,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    create_timestamp: chrono::DateTime<Utc>
}



async fn worker(worker_no: usize) -> Result<(), Box<dyn Error>> {
    println!("Worker {}: Starting...", worker_no);
    // create DB client
    let client = Client::with_uri_str(DB_URL).await?;

    loop {
        // put the thread to sleep
        println!("Worker {}: Going sleep", worker_no);
        let dur = WORKER_SLEEP_DURATION * (worker_no as u64);
        sleep(Duration::from_millis(dur)).await;
        println!("Worker {}: Running after sleep", worker_no);

        // query into database to fetch unvisited url 
        let filter = doc!{"visited": false};
        let db = client.database(DB_NAME);
        let coll_url = db.collection::<AllUrl>(URL_COLL);
        let res = coll_url.find_one(filter, None).await;
        if res.is_err() {
            println!("Worker {}: Error- {:?}", worker_no, res.err().unwrap());
            continue;
        }
        let res = res.unwrap();
        if res.is_none() {
            println!("Worker {}: Unvisited URL not found", worker_no);
            continue;
        }

        // update the database with visited flag to true
        // so that no other thred pickup the same url
        let res = res.unwrap();
        let filter = doc!{ "_id": res._id, "visited": false };
        let update = doc! { "$set": { "visited": true } };
        let update_result = coll_url.update_one(filter, update, None).await;
        if update_result.is_err() {
            println!("Worker {}: Error- {:?}", worker_no, update_result.err().unwrap());
            continue;
        }
        let update_result = update_result.unwrap();
        if update_result.modified_count == 0 {
            println!("Worker {}: Not able to update URL visited list", worker_no);
            continue;
        }

        // if update was successful then we can proceed to crawl the web page
        let response = visit_url(&res.url).await;
        // extract data from the web page
        let re = Regex::new(r"[^\u0980-\u09FF]")?;
        let resp = re.replace_all(&response, " ");
        let resp = resp
        .split_whitespace()
        .map(|s| s.trim().to_string())
        .collect::<HashSet<String>>();
        

        // let mut words = Vec::new();
        let coll_word = db.collection::<AllWord>(WORD_COLL);
        for s in resp {
            let hashsed = create_hash(&s, Sha256::default());
            let f = doc! { "hash": hashsed.to_owned() };
            let update = doc! {
                "$set": {
                    "val": s,
                    "hash": hashsed,
                    "createTimestamp": Utc::now(),
                }
            };
            let options = mongodb::options::UpdateOptions::builder().upsert(true).build();
            let r = coll_word.update_one(f, update, options).await;
            match r {
                Ok(rslt) => {
                    println!("Worker {}: Inserted Word {:?}", worker_no, rslt);
                },
                Err(e) => {
                    println!("Worker {}: Error - Insert to allWords failed. {:?}", worker_no, e);
                }
            }
        }



        // extract links 
        let orig_url = reqwest::Url::parse(&res.url);
        if orig_url.is_err() {
            println!("Worker {}: Not able to parse url: {:?}", worker_no, orig_url.err().unwrap());
            continue;
        }
        let orig_url = orig_url.unwrap();
        let host = orig_url.host_str();
        if host.is_none() {
            println!("Worker {}: Error host not found. URL: {}", worker_no, &res.url);
            continue;
        }
        let host = host.unwrap();
        let scheme = orig_url.scheme();
        let links = get_list(&response);

        // format all links in the web page and collect into a vector
        let mut v = Vec::new();
        for link in links {
            if link.starts_with("https") || link.starts_with("http") {
                if let Ok(u) = reqwest::Url::parse(&link) {
                    let d = doc! {
                        "url": u.to_string(),
                        "visited": false,
                    };
                    v.push(d);
                }
                continue;
            }

            if link.starts_with("/") {
                let s = format!("{}://{}{}", scheme, host, link);
                if let Ok(u) = reqwest::Url::parse(&s) {
                    let d = doc! {
                        "url": u.to_string(),
                        "visited": false,
                    };
                    v.push(d);
                }
                continue;
            }

            let s = format!("{}://{}/{}", scheme, host, link);
            if let Ok(u) = reqwest::Url::parse(&s) {
                let d = doc! {
                    "url": u.to_string(),
                    "visited": false,
                };
                v.push(d);
            }
        }

        // for each element in the vector insert into the database
        // if the operation failed for unique index violation then simply ignore and continue
        for doc in v {
            let coll = db.collection::<Document>(URL_COLL);
            let r = coll.insert_one(doc, None).await;
            match r {
                Ok(rslt) => {
                    println!("Worker {}: URL inserted {:?}", worker_no, rslt);
                },
                Err(e) => {
                    println!("Worker {}: URL insert error {:?}", worker_no, e);
                }
            }
        }
    }

}

fn create_hash<D>(msg: &str, mut hasher: D) -> String
where
    D: Digest,
    Output<D>: std::fmt::LowerHex,
{
    hasher.update(msg);
    format!("{:x}", hasher.finalize())
}


async fn visit_url(url: &str) -> String {
    println!("Visit URL: {}", url);
    let resp = reqwest::get(url).await;
    match resp {
        Err(e) => {
            println!("{:?}", e);
            "".to_string()
        },
        Ok(res) => {
            match res.text().await {
                Ok(r) => { r },
                Err(e) => {
                    println!("{:?}", e);
                    "".to_string()
                }
            }
        }
    }
}


fn get_list(resp: &str) -> HashSet<String> {
    let document = select::document::Document::from(resp);
    document
    .find(select::predicate::Name("a"))
    .filter_map(|n| n.attr("href"))
    .map(str::to_string)
    .collect::<HashSet<String>>()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    println!("Hello, world!");


    let handles = (0..10).map(|n|{
        task::spawn(async move {
            let r = worker(n + 1).await;
            if r.is_err() {
                println!("Error in worker thred {:?}", r.err().unwrap());
            }
        })
    }).collect::<Vec<_>>();

    for handle in handles {
        handle.await?;
    }

    Ok(())

}
