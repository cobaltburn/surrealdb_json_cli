use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use csv::Writer;
use serde_json::{Map, Value};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::str::FromStr;
use surrealdb::engine::remote::http::{Client, Http};
use surrealdb::opt::auth::Root;
use surrealdb::{sql, Surreal};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

const ENDPOINT: &str = "127.0.0.1:8000";
const USER: &str = "root";
const PASS: &str = "root";
const NS: &str = "test";
const DB: &str = "test";
const FILEFORMAT: &str = "json";

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser)]
struct Import {
    #[arg(short, long, default_value = ENDPOINT )]
    endpoint: String,

    #[arg(short, long, default_value = USER)]
    user: String,

    #[arg(short, long, default_value = PASS)]
    pass: String,

    #[arg(long, default_value = NS)]
    ns: String,

    #[arg(long, default_value = DB)]
    db: String,

    #[arg(value_name = "files", value_parser = clap::value_parser!(PathBuf), num_args = 1.., required = true)]
    files: Vec<PathBuf>,
}

#[derive(Parser)]
struct Export {
    #[arg(short, long, default_value = ENDPOINT )]
    endpoint: String,

    #[arg(short, long, default_value = USER)]
    user: String,

    #[arg(short, long, default_value = PASS)]
    pass: String,

    #[arg(long, default_value = NS)]
    ns: String,

    #[arg(long, default_value = DB)]
    db: String,

    #[arg(short, long, default_value = FILEFORMAT)]
    format: String,

    #[arg(value_name = "tables", value_parser = clap::value_parser!(String), num_args = 1.., required = true)]
    tables: Vec<String>,
}

#[derive(Subcommand)]
enum Commands {
    Import(Import),
    Export(Export),
}

// defaults are for my own sanity
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {}

enum FileFormat {
    Json,
    Csv,
}

impl FromStr for FileFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "json" => Ok(FileFormat::Json),
            "csv" => Ok(FileFormat::Csv),
            e => Err(anyhow!("{} is an invalid file format", e)),
        }
    }
}

impl<'a> TryFrom<&'a OsStr> for FileFormat {
    type Error = anyhow::Error;

    fn try_from(value: &'a OsStr) -> Result<Self> {
        match value.to_str() {
            Some("json") => Ok(FileFormat::Json),
            Some("csv") => Ok(FileFormat::Csv),
            Some(e) => Err(anyhow!("{} is an invalid file format", e)),
            None => Err(anyhow!("unable to parser file format")),
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Import(args) => import(args).await.unwrap(),
        Commands::Export(args) => export(args).await.unwrap(),
    }
}

async fn connect_db(
    address: &str,
    username: &str,
    password: &str,
    ns: &str,
    db_name: &str,
) -> Result<Surreal<Client>> {
    let db = Surreal::new::<Http>(address).await?;
    db.signin(Root { username, password }).await?;
    db.use_ns(ns).use_db(db_name).await?;
    Ok(db)
}

async fn import(args: Import) -> Result<()> {
    let db = connect_db(&args.endpoint, &args.user, &args.pass, &args.ns, &args.db).await?;

    match file_format(&args.files)? {
        FileFormat::Json => {
            for path in args.files {
                import_json(path, &db).await?;
            }
        }
        FileFormat::Csv => todo!(),
    }
    Ok(())
}

fn file_format(files: &Vec<PathBuf>) -> Result<FileFormat> {
    for path in files.iter() {
        if !path.is_file() {
            return Err(anyhow!("error: {:#?} is not a file", path));
        };
    }
    let ext = files
        .first()
        .unwrap()
        .extension()
        .context("Error: no file extention was found")?;

    if !files.iter().all(|p| p.extension().unwrap() == ext) {
        return Err(anyhow!("error: not all files are the same type"));
    }

    FileFormat::try_from(ext)
}

async fn import_json(path: PathBuf, db: &Surreal<Client>) -> Result<()> {
    let (insert_query, records) = generate_insert(path).await?;
    let mut response = db.query(insert_query).bind(("records", &records)).await?;
    assert!(response.take_errors().is_empty());
    Ok(())
}

async fn generate_insert(path: PathBuf) -> Result<(String, Vec<Value>)> {
    let records = {
        let json = fs::read_to_string(&path).await?;
        serde_json::from_str::<Vec<Value>>(&json)
            .unwrap_or(vec![serde_json::from_str::<Value>(&json)?])
    };

    let table = path
        .file_stem()
        .context("failed to parse stem")?
        .to_str()
        .context("failed to convert &OsStr")?;

    let query = format!("INSERT INTO {} $records", table);
    Ok((query, records))
}

async fn export(args: Export) -> Result<()> {
    let format = FileFormat::from_str(&args.format)?;
    let db = connect_db(&args.endpoint, &args.user, &args.pass, &args.ns, &args.db).await?;

    let mut handles = JoinSet::new();
    for table in args.tables {
        let db = db.clone();
        let table = table.clone();
        match format {
            FileFormat::Json => {
                handles.spawn(async move { export_table_as_json(&table, &db).await });
            }
            FileFormat::Csv => {
                handles.spawn(async move { export_table_as_csv(&table, &db).await });
            }
        }
    }
    while let Some(_) = handles.join_next().await {}
    Ok(())
}

async fn export_table_as_json(table: &str, db: &Surreal<Client>) -> Result<()> {
    let file_name = PathBuf::from(format!("{}.json", table));
    let records = select_table(table, db).await?;

    let json = serde_json::to_string_pretty(&records)?;
    let mut file = fs::File::create(file_name).await?;
    file.write_all(json.as_bytes()).await?;
    Ok(())
}

// TODO add check to convert any record ids
async fn select_table(table: &str, db: &Surreal<Client>) -> Result<Vec<Value>> {
    let table: Vec<Value> = db.select(table).range(1..2).await?;
    let table: Vec<Value> = table
        .into_iter()
        .map(|mut record| {
            let Some(record) = convert_id(&mut record) else {
                return record;
            };
            record.to_owned()
        })
        .collect();
    Ok(table)
}

fn convert_id(record: &mut Value) -> Option<&Value> {
    let id = record.get_mut("id")?;
    let thing: sql::Thing = serde_json::from_value(id.clone()).ok()?;
    *id = Value::String(format!("{}", thing.id));
    Some(record)
}

async fn export_table_as_csv(table: &str, db: &Surreal<Client>) -> Result<()> {
    let records = select_table(table, db).await?;
    let fields = extract_fields(table, db).await?;
    let records = records_to_csv(&fields, records);

    let file_name = PathBuf::from(format!("{}.csv", table));
    let file = std::fs::File::create(file_name)?;
    let mut wtr = Writer::from_writer(file);

    wtr.write_record(fields)?;
    for record in records {
        wtr.write_record(record)?;
    }
    wtr.flush()?;

    Ok(())
}

async fn extract_fields(table: &str, db: &Surreal<Client>) -> Result<Vec<String>> {
    let mut response = db.query(format!("INFO FOR TABLE {}", table)).await?;
    let mut table = response
        .take::<Option<Value>>(0)?
        .context("failed to return table info")?;

    let fields = serde_json::from_value::<Map<String, Value>>(table["fields"].take())?;
    let fields: Vec<String> = fields.into_iter().map(|(field, _)| field).collect();
    Ok(fields)
}

fn records_to_csv(fields: &Vec<String>, records: Vec<Value>) -> Vec<Vec<String>> {
    let format_record = |rec: Value| {
        fields
            .iter()
            .map(|field| {
                if let Some(val) = rec.get(field) {
                    value_to_string(val)
                } else {
                    String::from("NULL")
                }
            })
            .collect()
    };

    records.into_iter().map(format_record).collect()
}

fn value_to_string(val: &Value) -> String {
    match val {
        Value::Null => String::from("NULL"),
        Value::Bool(e) => e.to_string(),
        Value::Number(e) => e.to_string(),
        Value::String(e) => e.to_owned(),
        Value::Array(e) => serde_json::to_string(e).unwrap(),
        Value::Object(e) => serde_json::to_string(e).unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;
    use surrealdb::sql;

    #[tokio::test]
    async fn test_fields() {
        let db = connect_db(ENDPOINT, USER, PASS, NS, DB).await.unwrap();
        let mut resp = db.query("INFO FOR TABLE pokemon").await.unwrap();
        let mut x = resp.take::<Option<Value>>(0).unwrap().unwrap();
        let x: Map<String, Value> = serde_json::from_value(x["fields"].take()).unwrap();
        let mut fields = vec![];
        for (field, _) in x {
            fields.push(field)
        }
        println!("{:?}", fields)
    }

    #[test]
    fn test_json() {
        let path = PathBuf::from("./results.json");
        let json = std::fs::read_to_string(&path).unwrap();
        let x = sql::json(&json).unwrap();
        let x: Vec<sql::Value> = Vec::try_from(x).unwrap();
        x.iter().for_each(|e| println!("{e:#?}"));
    }

    #[tokio::test]
    async fn test_pull() {}

    #[tokio::test]
    async fn check_db() {
        let db = connect_db(ENDPOINT, USER, PASS, NS, DB).await.unwrap();
        let mut resp = db.query(r#"INFO FOR DB"#).await.unwrap();
        println!("{:?}", resp);
        let x: Vec<Value> = resp.take(0).unwrap();

        println!("{:?}", x);
    }

    #[tokio::test]
    async fn test_export() {
        let db = connect_db(ENDPOINT, USER, PASS, NS, DB).await.unwrap();
        let x = select_table("pokemon", &db).await.unwrap();
        println!("{:?}", x)
    }

    #[tokio::test]
    async fn test_table_save() {
        let db = connect_db(ENDPOINT, USER, PASS, NS, DB).await.unwrap();
        export_table_as_json("empty_pokemon", &db).await.unwrap();
    }
}
