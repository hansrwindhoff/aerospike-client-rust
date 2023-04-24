// #[macro_use]
extern crate aerospike;
use aerospike::Value;
use itertools::Itertools;
use serde_json::Result;
use std::any::Any;
use std::env;
use std::io;
use std::time::Instant;

use aerospike::{
    // Bins,
    Client,
    ClientPolicy,
    // QueryPolicy,
    ScanPolicy,
    // WritePolicy
    // ReadPolicy,
    // Statement,
};
// use aerospike::operations;

fn main() {
    let mut wtr = csv::Writer::from_writer(io::stdout());

    let cpolicy = ClientPolicy::default();
    // let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("0.0.0.0:3000"));
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("aerospk1.dca1.lijit.com:3000"));
    let client = Client::new(&cpolicy, &hosts).expect("Failed to connect to cluster");

    let now = Instant::now();
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
    // pol.max_concurrent_nodes = 6;

    match client.scan(
        &pol,
        "addelivery",
        "publisher_domain_block",
        ["publisher_id", "domain", "battr", "bcat", "badv"],
    ) {
        //Bins::All
        // match client.scan(&pol, "test", "scala_input_data", Bins::All) {
        Ok(records) => {
            // let mut count = 0;

            // records.for_each(
            //     |r|{
            //         match r {
            //         Ok(r)=> println!("r: {}", r),
            //         Err(err)=>println!("Failed to execute scan: {}", err),

            //         }

            //      })

            for (idx, record) in records.enumerate() {
                match record {
                    Ok(record) => {
                        // println!("r:  {:#?}", record.bins);
                        let new_map = record.bins.keys().sorted();
                        if idx == 0 {
                            match wtr.write_record(new_map.clone()){
                                Err(_err)=>{panic!("failed to write headers")}
                                Ok(_) => (),
                            };
                        }
                        let vals = new_map.map(|k| {
                            let val = record.bins.get(k);
                            match val {
                                Some(v) => {
                                    // println!("{:?}", v.type_id());
                                    v.as_string()
                                },
                                None => "".to_string(),
                            }
                        });
                        let strg_vals = vals.map(|v| v);

                        
                        match wtr.write_record(strg_vals){
                            Err(_err)=>{panic!("failed to write row")}
                            Ok(_) => (),
                        };

                        // count += 1
                    }
                    Err(err) => panic!("Error executing scan: {}", err),
                }
            }
            // wtr.write_record(&(records.take(10)));
            // println!("Records: {}", count);
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
