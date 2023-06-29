// #[macro_use]
extern crate aerospike;
use aerospike::Value;

use std::any::{type_name, Any, TypeId};

fn type_of<T>(_: T) -> &'static str {
    type_name::<T>()
}
fn main() {
    let t1: Value = "hello".into();
    let t2: [Value; 2] = ["hello".into(), "hello2".into()];
    let x = 21;
    let y = 2.5;
    println!("{}", type_of(&y));
    println!("{}", type_of(x));
    // println!("{}", type_of(t1));

    if  t1.type_id() == TypeId::of::<Value>() {
        println!("{:?}",  t1.type_id());
        println!(" TypeId::of::<Value>(){:?}",   TypeId::of::<Value>());
    }

    if  t2.type_id() == TypeId::of::<[Value]>() {
        println!("{:?}",  t2.type_id());
        println!(" TypeId::of::<[Value]>(){:?}",   TypeId::of::<[Value]>());
    }
}
