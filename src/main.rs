extern crate oracle;

use std::result::Result;
use std::fs::File;
use std::io::prelude::*;

use oracle::*;

fn main() {

    let mut username = String::new();
    File::open("username")
        .expect("username file not found")
        .read_to_string(&mut username)
        .expect("something went wrong reading the file");

    let mut password = String::new();
    File::open("password")
        .expect("username file not found")
        .read_to_string(&mut password)
        .expect("something went wrong reading the file");

    // Connect to a database.
    let connection = Connection::connect(&username, &password, "//pg-uni-oradbe-02.unbc.ca:1521/bdev09", &[])
        .unwrap();

    let tables_result = get_tables(&connection)
        .unwrap();

    let tables : std::vec::Vec<&str> = tables_result
        .iter()
        .filter(|table_name| {
            table_name.as_str() != "EXCEPTIONS"
        })
        .map(AsRef::as_ref)
        .collect();
    
    println!("----- TABLES -----");
    tables
    .iter()
    .for_each(|data| {
        println!("{}", data);
    });
    println!("------------------");


    let column_data = get_column_data(&connection, &tables)
        .unwrap();
    
    column_data
    .iter()
    .for_each(|data| {
        match data {
            Some(result_set) => {
                let column_info = result_set.column_info();
                print!("{}", column_info[0].);
                println!("")
            },
            None => ()
        }
    });

    match connection.close() {
        Ok(()) => {
            println!("Connection closed")
        },
        Err(reason) => {
            println!("Error closing connection: {}", reason)
        },
    }

}

fn get_tables<'a>(connection : &'a Connection) -> Result<Vec<String>, oracle::Error> {

    // SQL query
    let sql = "SELECT TABLE_NAME FROM DBA_TABLES WHERE OWNER = 'WTAILOR' ORDER BY OWNER ASC";

    // Print the first row.
    let results = connection.query(sql, &[])?;
    
    let tables : Vec<String> = results.map(|result| -> String {
        result
        .unwrap()
        .get(0)
        .unwrap()
    }).collect();

    Ok(tables)
}

fn get_column_data<'a, 'b>(connection: &'a Connection, tables: &'b Vec<&str>) -> Result<Vec<Option<ResultSet<'a, oracle::Row>>>, oracle::Error> {


    let column_data = tables
    .iter()
    .map(|table| {
        // SQL query
        let sql = format!("SELECT * FROM {} WHERE '0' = '1'", table);
        let result = connection.query(sql.as_str(), &[]);
        match result {
            Ok(result) => {
                Some(result)
            },
            Err(reason) => {
                println!("Error querying {}, reason: {}", table, reason);
                None
            },
        }
    })
    .collect();

    Ok(column_data)
}