use anyhow::{Context, Result};
use clap::Parser;
use serde_json::value::Value;
use std::fs;
use std::path::PathBuf;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    // #[arg(short, long)]
    // endpoint: String,

    // #[arg(short, long)]
    // user: String,

    // #[arg(short, long)]
    // pass: String,

    // #[arg(long)]
    // ns: String,

    // #[arg(long)]
    // db: String,
    #[arg(value_name = "FILES", value_parser = clap::value_parser!(PathBuf), num_args = 1.., required = true)]
    files: Vec<PathBuf>,
}

enum FileType {
    Json,
    Invalid(String),
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:?}", args);

    let file_type = file_type(&args);

    let db = connect_db("localhost:8000".into(), "root", "root", "test", "test")
        .await
        .unwrap();

    match file_type {
        FileType::Json => {
            for path in args.files {
                import_json(path, &db).await.unwrap();
            }
        }
        FileType::Invalid(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    }
}

fn file_type(args: &Args) -> FileType {
    for path in args.files.iter() {
        if !path.is_file() {
            return FileType::Invalid(format!("error: {:?} is not a file", path));
        };
        if path.extension().unwrap() != "json" {
            return FileType::Invalid(format!("error: {:?} is an invalid file type", path));
        };
    }
    let ext = args.files.first().unwrap().extension().unwrap();

    if !args.files.iter().all(|p| p.extension().unwrap() == ext) {
        return FileType::Invalid(format!("error: not all files are the same type"));
    }

    FileType::Json
}

async fn connect_db(
    address: String,
    username: &str,
    password: &str,
    ns: &str,
    db_name: &str,
) -> Result<Surreal<Client>> {
    let db = Surreal::new::<Ws>(address).await?;

    db.signin(Root { username, password }).await?;

    db.use_ns(ns).use_db(db_name).await?;
    Ok(db)
}

fn generate_query(path: PathBuf) -> Result<(String, Vec<Value>)> {
    let records = {
        let json = fs::read_to_string(&path)?;
        serde_json::from_str::<Vec<Value>>(&json)
            .unwrap_or(vec![serde_json::from_str::<Value>(&json)?])
    };
    let table = path
        .file_stem()
        .context("failed to parse stem")?
        .to_str()
        .context("failed to conert &OsStr")?;

    let query = format!("INSERT INTO {} $records", table);
    Ok((query, records))
}

async fn import_json(path: PathBuf, db: &Surreal<Client>) -> Result<()> {
    let (query, records) = generate_query(path)?;
    let mut response = db.query(query).bind(("records", &records)).await?;
    assert!(response.take_errors().is_empty());

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use surrealdb::{engine::local::Mem, sql::Thing};

    use super::*;

    #[tokio::test]
    async fn test_json_import() {
        let path = PathBuf::from("./person.json");

        let (query, records) = generate_query(path.clone()).unwrap();

        let db = Surreal::new::<Mem>(()).await.unwrap();
        db.use_ns("test").use_db("test").await.unwrap();

        let mut response = db.query(query).bind(("records", &records)).await.unwrap();
        assert!(response.take_errors().is_empty());

        let rec = {
            let json = fs::read_to_string(&path).unwrap();
            serde_json::from_str::<Vec<Value>>(&json)
                .unwrap_or(vec![serde_json::from_str::<Value>(&json).unwrap()])
        };

        let table = path.file_stem().unwrap().to_str().unwrap();
        let res: Vec<Value> = db.select(table).await.unwrap();

        let rec: Vec<_> = rec
            .into_iter()
            .map(|mut v| {
                let id = v.get_mut("id").unwrap();
                let v_id = id.as_i64().unwrap();
                *id = json!(Thing {
                    tb: table.into(),
                    id: v_id.into()
                });
                v
            })
            .collect();

        assert_eq!(rec, res);
    }
}
