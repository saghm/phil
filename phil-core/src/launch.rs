use std::{ffi::OsString, path::PathBuf, process::Child, time::Duration};

use bson::{bson, doc, Bson, Document};
use monger_core::Monger;
use mongodb::{
    options::{auth::Credential, ClientOptions, StreamAddress, Tls},
    Client,
};
use serde::Deserialize;

use crate::{
    cluster::TlsOptions,
    error::{Error, Result},
};

pub(crate) fn stream_address(host: &str, port: u16) -> StreamAddress {
    StreamAddress {
        hostname: host.into(),
        port: port.into(),
    }
}

#[derive(Debug)]
pub(crate) enum ServerShardType {
    Config,
    #[allow(unused)]
    Shard,
    None,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommandResponse {
    pub ok: f64,
    pub code_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplSetStatus {
    members: Vec<ReplSetMember>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplSetMember {
    state_str: String,
}

fn add_tls_options(args: &mut Vec<OsString>, tls_options: Option<&TlsOptions>) {
    if let Some(tls_options) = tls_options {
        args.extend_from_slice(&[
            OsString::from("--tlsMode"),
            OsString::from("requireTLS"),
            OsString::from("--tlsCAFile"),
            OsString::from(&tls_options.ca_file_path),
            OsString::from("--tlsCertificateKeyFile"),
            OsString::from(&tls_options.server_cert_file_path),
        ]);

        if tls_options.weak_tls {
            args.push(OsString::from("--tlsAllowConnectionsWithoutCertificates"));
        }
    }
}

fn configure_repl_set(client: &Client, config: Document) -> Result<()> {
    let db = client.database("admin");
    let response = db.run_command(
        doc! {
            "replSetInitiate": config.clone(),
        },
        None,
    )?;

    let CommandResponse { ok, code_name } = bson::from_bson(Bson::Document(response.clone()))?;

    if ok == 1.0 {
        return Ok(());
    }

    if let Some(code_name) = code_name {
        if code_name != "AlreadyInitialized" {
            return Err(Error::ReplicaSetConfigError { response });
        }
    }

    db.run_command(
        doc! {
            "replSetReconfig": config,
        },
        None,
    )?;

    loop {
        std::thread::sleep(Duration::from_millis(500));

        let response = db.run_command(doc! { "replSetGetStatus": 1 }, None)?;
        let ReplSetStatus { members } = bson::from_bson(Bson::Document(response))?;
        if members.iter().any(|member| member.state_str == "PRIMARY") {
            return Ok(());
        }
    }
}

pub(crate) fn mongos(
    monger: &Monger,
    processes: &mut Vec<Child>,
    version_id: &str,
    port: u16,
    config_port: u16,
    config_name: &str,
    shard_ports: impl IntoIterator<Item = u16>,
    tls_options: Option<&TlsOptions>,
    shard_names: Option<Vec<String>>,
    auth: Option<Credential>,
) -> Result<Client> {
    let mut args = vec![
        OsString::from("--port"),
        OsString::from(port.to_string()),
        OsString::from("--configdb"),
        OsString::from(format!("{}/localhost:{}", config_name, config_port)),
    ];

    add_tls_options(&mut args, tls_options);

    if auth.is_some() {
        args.push(OsString::from("--auth"));
    }

    processes.push(monger.run_background_command("mongos", args, version_id)?);

    let mut options = ClientOptions::builder()
        .hosts(vec![stream_address("localhost".into(), port)])
        .credential(auth)
        .build();

    if let Some(tls_options) = tls_options {
        options.tls = Some(Tls::from(tls_options.clone()));
    }

    let client = Client::with_options(options)?;

    let mut shard_names = shard_names.map(IntoIterator::into_iter);

    for shard_port in shard_ports {
        let shard_name = match shard_names.as_mut().and_then(Iterator::next) {
            Some(name) => format!("{}/localhost:{}", name, shard_port),
            None => format!("localhost:{}", shard_port),
        };

        let response = client
            .database("admin")
            .run_command(doc! { "addShard": shard_name }, None)?;

        let CommandResponse { ok, .. } = bson::from_bson(Bson::Document(response.clone()))?;

        if ok != 1.0 {
            return Err(Error::AddShardError { response });
        }
    }

    Ok(client)
}

pub(crate) fn single_server(
    monger: &Monger,
    version_id: &str,
    port: u16,
    path: Option<PathBuf>,
    tls_options: Option<&TlsOptions>,
    shard_server: bool,
    auth: bool,
) -> Result<Child> {
    let mut args: Vec<_> = vec![OsString::from("--port"), OsString::from(port.to_string())];

    if let Some(path) = path {
        args.push(OsString::from("--dbpath"));
        args.push(path.into_os_string());
    }

    if shard_server {
        args.push(OsString::from("--shardsvr"));
    }

    if auth {
        args.push(OsString::from("--auth"));
    }

    add_tls_options(&mut args, tls_options);
    let child = monger.start_mongod(args, version_id, false)?;

    Ok(child)
}

pub(crate) fn replica_set(
    monger: &Monger,
    processes: &mut Vec<Child>,
    version_id: &str,
    hosts: Vec<StreamAddress>,
    set_name: &str,
    mut paths: impl Iterator<Item = PathBuf>,
    tls_options: Option<&TlsOptions>,
    server_shard_type: ServerShardType,
    auth: Option<Credential>,
) -> Result<Client> {
    for host in &hosts {
        let mut args = vec![
            OsString::from("--port".to_string()),
            OsString::from(host.port.unwrap().to_string()),
            OsString::from("--replSet"),
            OsString::from(set_name),
        ];

        if let Some(path) = paths.next() {
            args.push(OsString::from("--dbpath"));
            args.push(path.into_os_string());
        }

        match server_shard_type {
            ServerShardType::Config => args.push(OsString::from("--configsvr")),
            ServerShardType::Shard => args.push(OsString::from("--shardsvr")),
            ServerShardType::None => {}
        };

        if auth.is_some() {
            args.push(OsString::from("--auth"));
        }

        add_tls_options(&mut args, tls_options);
        processes.push(monger.start_mongod(args, version_id, false)?);
    }

    let config = doc! {
        "_id": set_name.clone(),
        "members": hosts.iter().enumerate().map(|(i, host)| {
            Bson::Document(
                doc! {
                    "_id": i as i32,
                    "host": host.to_string(),
                }
            )
        }).collect::<Vec<_>>()
    };

    let mut options = ClientOptions::builder()
        .hosts(hosts)
        .repl_set_name(set_name.to_string())
        .credential(auth)
        .build();

    if let Some(tls_options) = tls_options {
        options.tls = Some(Tls::from(tls_options.clone()));
    }

    let mut direct_options = options.clone();
    direct_options.hosts.split_off(1);
    direct_options.direct_connection = Some(true);
    direct_options.repl_set_name.take();
    let direct_client = Client::with_options(direct_options)?;

    configure_repl_set(&direct_client, config)?;

    let client = Client::with_options(options)?;

    Ok(client)
}
