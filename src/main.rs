use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use http_body_util::Full;
use http_body_util::{combinators::BoxBody, BodyExt, Empty};
use hyper::server::conn::http1;
use hyper::{body::Bytes, service::service_fn, Method, Request, Response, StatusCode};
use serde::Serialize;
use serde::ser::{SerializeStruct,Serializer};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpListener;

fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

fn not_found_response() -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    let mut not_found = Response::new(empty());
    *not_found.status_mut() = StatusCode::NOT_FOUND;
    Ok(not_found)
}

fn bad_request_response(msg: &str) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    let mut resp = Response::new(full(msg.to_string()));
    *resp.status_mut() = StatusCode::BAD_REQUEST;
    Ok(resp)
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[derive(Debug)]
struct Kv {
    key: String,
    value: AttributeValue,
}

fn value_to_string(value: &AttributeValue) -> String {
    format!("{:?}", value)
}

impl Serialize for Kv {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("Kv", 2)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", &value_to_string(&self.value))?;
        state.end()
    }
}

async fn insert_new_account(
    client: &Client,
    gsid: &str,
    id: &str,
) -> Result<String, aws_sdk_dynamodb::Error> {
    let gsid_av = AttributeValue::S(gsid.to_string());

    let request = client
        .put_item()
        .table_name("accounts")
        .item("gsid", gsid_av)
        .item("activated", AttributeValue::Bool(false))
        .item("id", AttributeValue::S(id.to_string()));
    let _ = request.send().await?;
    // ideally I would get the attributes from the result of `request.send().await` but that doesn't seem to work
    // this case isn't super common hopefully
    get_account(client, gsid).await
}

async fn get_account(client: &Client, gsid: &str) -> Result<String, aws_sdk_dynamodb::Error> {
    let results = client
        .query()
        .table_name("accounts")
        .key_condition_expression("#gsid = :gsid")
        .expression_attribute_names("#gsid", "gsid")
        .expression_attribute_values(":gsid", AttributeValue::S(gsid.to_string()))
        .send()
        .await?;

    if let Some(items) = results.items {
        assert!(items.len() == 1);        
        serialize_account(&items[0])
    } else {
        panic!() // TODO
    }
}

fn serialize_account(attributes: &HashMap<String, AttributeValue>) -> Result<String,aws_sdk_dynamodb::Error> {
    let combined : Vec<_> = attributes.iter().map(|h| Kv{key:h.0.to_string(),value:h.1.clone()}).collect();
    let json = serde_json::to_string(&combined);
    Ok(json.expect("a json"))
}

async fn insert_new_gsid(client: &Client, id: &str) -> Result<String, aws_sdk_dynamodb::Error> {
    let id_av = AttributeValue::S(id.to_string());

    let hash = calculate_hash(&id.to_string());
    let gsid: String = format!("GS_{}", hash);
    let gsid_av = AttributeValue::S(gsid.clone());

    let request = client
        .put_item()
        .table_name("userids")
        .item("platformid", id_av)
        .item("gsid", gsid_av);
    let _ = request.send().await?;
    Ok(gsid)
}

async fn get_gsid(client: &Client, id: &str) -> Result<String, aws_sdk_dynamodb::Error> {
    let results = client
        .query()
        .table_name("userids")
        .key_condition_expression("#id = :id")
        .expression_attribute_names("#id", "platformid")
        .expression_attribute_values(":id", AttributeValue::S(id.to_string()))
        .send()
        .await?;

    if let Some(items) = results.items {
        match items.len() {
            1 => {
                let gsid = items[0]["gsid"].as_s().expect("a string").clone();
                get_account(&client, &gsid).await
            }
            0 => {
                let gsid = insert_new_gsid(&client, id).await.expect("gsid");
                insert_new_account(&client, &gsid, id).await
            }
            _ => panic!(), // TODO
        }
    } else {
        panic!() // TODO
    }
}

async fn hello(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/login") => {
            if req.headers().contains_key("id") {
                let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
                let config = aws_config::from_env().region(region_provider).load().await;
                let client = Client::new(&config);

                let id = &req.headers()["id"].to_str().expect("a str");
                let gsid = get_gsid(&client, id).await;
                Ok(Response::new(full(gsid.expect("gsid"))))
            } else {
                bad_request_response("missing id")
            }
        }
        _ => not_found_response(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(stream, service_fn(hello))
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
