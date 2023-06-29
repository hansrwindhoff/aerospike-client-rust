// #[macro_use]
extern crate aerospike;
// use aerospike::Value;
use itertools::Itertools;
use serde_json::value;
// use serde_json::Result;
// use std::any::Any;
use std::env;
use std::io;
// use std::time::Instant;

use aerospike::{
    Client,
    ClientPolicy,
    // QueryPolicy,
    ScanPolicy,
    // WritePolicy
    // ReadPolicy,
    // Statement,
    // Bins,
    Value,
};
// use aerospike::operations;

fn main() {
    let mut wtr = csv::Writer::from_writer(io::stdout());

    let cpolicy = ClientPolicy::default();

    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("0.0.0.0:3000"));
    // let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("aerospk1.dca1.lijit.com:3000"));
    // let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("0.0.0.0:3000"));

    let client = Client::new(&cpolicy, &hosts).expect("Failed to connect to cluster");

    // let now = Instant::now();
    // let rpolicy = ReadPolicy::default();
    // // let wpolicy = WritePolicy::default();
    // let key = as_key!("test", "test", "test");
    // let blkkey = as_key!("test", "demo", "257618");

    // let bins = [
    //     as_bin!("int", 999),
    //     as_bin!("str", "Hello, World!"),
    // ];
    // client.put(&wpolicy, &key, &bins).unwrap();
    // let rec = client.get(&rpolicy, &key, Bins::All);
    // println!("Record: {}", rec.unwrap());

    // client.touch(&wpolicy, &key).unwrap();
    // let rec = client.get(&rpolicy, &key, Bins::All);
    // println!("Record: {}", rec.unwrap());

    // let rec = client.get(&rpolicy, &key, Bins::None);
    // println!("Record Header: {}", rec.unwrap());

    // let blk_rec = client.get(&rpolicy, &blkkey,  ["publisher_id", "domain"]);
    // println!("Record Header: {}", blk_rec.unwrap());

    // let stmt = Statement::new("test", "scala_input_data", Bins::All);
    // match client.query(&QueryPolicy::default(), stmt) {
    // match client.scan(&ScanPolicy::default(),"test", "demo",  ["publisher_id", "domain"]) {
    //     Ok(records) => {
    //         for record in &*records {
    //             // .. process record
    //             println!("r: {}", record.unwrap());
    //             println!("------------>");

    //         }
    //     },
    //     Err(err) => println!("Error fetching record: {}", err),
    // }

    let mut pol = ScanPolicy::default();
    pol.max_concurrent_nodes = 2;

    match client.scan(
        &pol,
        "test",
        "demo",
        // "addelivery",
        // "publisher_domain_block",
        ["publisher_id", "domain", "battr", "bcat", "badv"],
    ) {
        //Bins::All
        Ok(records) => {
            let mut count = 0;

            // records.for_each(
            //     |r|{
            //         match r {
            //         Ok(r)=> println!("r: {}", r),
            //         Err(err)=>println!("Failed to execute scan: {}", err),

            //         }

            //      })

            for (idx, record) in records.take(50).enumerate() {
                match record {
                    Ok(record) => {
                        // println!("r:  {:#?}", record.bins);
                        let key_names = record.bins.keys().sorted();
                        if idx == 0 {
                            println!("key_names:  {}", key_names.clone().join(","));
                            match wtr.write_record(key_names.clone()) {
                                Err(_err) => {
                                    panic!("failed to write headers")
                                }
                                Ok(_) => (),
                            };
                        }
                        let vals = key_names.map(|k| {
                            let val = record.bins.get(k);
                            match val {
                                Some(v) => {
                                    count += 1;
                                    if let Value::Int(int) = v {
                                        // println!("int {:?}", int);
                                        int.to_string().clone()
                                    } else if let Value::Float(floaat) = v {
                                        // println!("float {:?}", floaat);
                                        floaat.to_string().clone()
                                    } else if let Value::Bool(bool) = v {
                                        // println!("bool {:?}", bool);
                                        bool.to_string().clone()
                                    } else if let Value::GeoJSON(geojson) = v {
                                        // println!("geojson {:?}", geojson);
                                        geojson.to_string().clone()
                                    } else if let Value::List(vec) = v {
                                        let temp = vec.iter().map(|v| v).join(",");
                                        // println!("list [{}]", temp);
                                        temp.to_string().clone()
                                    } else if let Value::String(strg) = v {
                                        // println!("string {}", strg);
                                        strg.to_string().clone()
                                    } else {
                                        "".to_string().clone()
                                    }
                                    
                                }
                                None => "".to_string().clone(),
                            }
                        });
                        let strg_vals = vals.into_iter().collect::<Vec<String>>().join( ",");
                        // println!(" wtf {:?}", strg_vals);

                        match wtr.write_field(strg_vals) {
                            Err(_err) => {
                                panic!("failed to write row")
                            }
                            Ok(_) => (),
                        };

                        // println!(" wtf {:?}", vals);
                        // println!(" wtf2 {:?} end wtf2 ", vals.into_iter().map(|v|  v));
                        // println!(" wtf3 {:?}", vals.into_iter().map(|v|  v));
                        // let strg_vals = vals.map(|v| v);
                    }
                    Err(err) => panic!("Error executing scan: {}", err),
                }
            }
            // wtr.write_record(&(records.take(10)));
            println!("Records: {}", count);
        }
        Err(err) => println!("Failed to execute scan: {}", err),
    }
    // let exists = client.exists(&wpolicy, &key).unwrap();
    // println!("exists: {}", exists);

    // let bin = as_bin!("int", "123");
    // let ops = &vec![operations::put(&bin), operations::get()];
    // let op_rec = client.operate(&wpolicy, &key, ops);
    // println!("operate: {}", op_rec.unwrap());

    // let existed = client.delete(&wpolicy, &key).unwrap();
    // println!("existed (should be true): {}", existed);

    // let existed = client.delete(&wpolicy, &key).unwrap();
    // println!("existed (should be false): {}", existed);

    // println!("total time: {:?}", now.elapsed());
}
