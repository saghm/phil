use std::{collections::VecDeque, ffi::OsString, path::PathBuf};

use monger_core::{process::ChildType, Monger};
use mongodb::{
    bson::{bson, doc, Bson, Document},
    options::{ClientOptions, Host},
    read_preference::ReadPreference,
    Client,
};
use serde::Deserialize;

use crate::{
    cluster::TlsOptions,
    error::{Error, Result},
};

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

fn add_tls_options(args: &mut Vec<OsString>, tls_options: Option<&TlsOptions>) {
    if let Some(tls_options) = tls_options {
        args.extend_from_slice(&[
            OsString::from("--tlsMode"),
            OsString::from("requireTLS"),
            OsString::from("--tlsCAFile"),
            OsString::from(&tls_options.ca_file_path),
            OsString::from("--tlsCertificateKeyFile"),
            OsString::from(&tls_options.cert_file_path),
            OsString::from("--tlsAllowConnectionsWithoutCertificates"),
        ]);
    }
}

fn configure_repl_set(client: &Client, config: Document) -> Result<()> {
    let db = client.database("admin");

    let response = db.run_command(
        doc! {
            "replSetInitiate": config.clone(),
        },
        Some(ReadPreference::Nearest {
            tag_sets: None,
            max_staleness: None,
        }),
    )?;

    let CommandResponse { ok, code_name } =
        mongodb::bson::from_bson(Bson::Document(response.clone()))?;

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
        Some(ReadPreference::Nearest {
            tag_sets: None,
            max_staleness: None,
        }),
    )?;

    Ok(())
}

pub(crate) fn mongos(
    monger: &Monger,
    port: u16,
    config_port: u16,
    config_name: &str,
    shard_ports: impl IntoIterator<Item = u16>,
    tls_options: Option<&TlsOptions>,
) -> Result<Client> {
    let mut args = vec![
        OsString::from("--port"),
        OsString::from(port.to_string()),
        OsString::from("--configdb"),
        OsString::from(format!("{}/localhost:{}", config_name, config_port)),
    ];

    add_tls_options(&mut args, tls_options);

    monger.command("mongos", args, "4.2.0", ChildType::Fork)?;

    let client = Client::with_options(
        ClientOptions::builder()
            .hosts(vec![Host::new("localhost".into(), Some(port))])
            .tls_options(tls_options.map(|opts| opts.clone().into()))
            .build(),
    )?;

    for shard_port in shard_ports {
        let response = client.database("admin").run_command(
            doc! { "addShard": format!("localhost:{}", shard_port) },
            None,
        )?;

        let CommandResponse { ok, .. } =
            mongodb::bson::from_bson(Bson::Document(response.clone()))?;

        if ok != 1.0 {
            return Err(Error::AddShardError { response });
        }
    }

    Ok(client)
}

pub(crate) fn single_server(
    monger: &Monger,
    port: u16,
    path: Option<PathBuf>,
    tls_options: Option<&TlsOptions>,
    shard_server: bool,
) -> Result<()> {
    let mut args: Vec<_> = vec![OsString::from("--port"), OsString::from(port.to_string())];

    if let Some(path) = path {
        args.push(OsString::from("--dbpath"));
        args.push(path.into_os_string());
    }

    if shard_server {
        args.push(OsString::from("--shardsvr"));
    }

    add_tls_options(&mut args, tls_options);
    monger.start_mongod(args, "4.2.0", ChildType::Fork)?;

    Ok(())
}

pub(crate) fn replica_set(
    monger: &Monger,
    hosts: Vec<Host>,
    set_name: &str,
    paths: impl IntoIterator<Item = PathBuf>,
    tls_options: Option<&TlsOptions>,
    server_shard_type: ServerShardType,
) -> Result<Client> {
    let mut paths: VecDeque<_> = paths.into_iter().collect();

    for host in &hosts {
        let mut args = vec![
            OsString::from("--port".to_string()),
            OsString::from(host.port.unwrap().to_string()),
            OsString::from("--replSet"),
            OsString::from(set_name),
        ];

        if let Some(path) = paths.pop_front() {
            args.push(OsString::from("--dbpath"));
            args.push(path.into_os_string());
        }

        match server_shard_type {
            ServerShardType::Config => args.push(OsString::from("--configsvr")),
            ServerShardType::Shard => args.push(OsString::from("--shardsvr")),
            ServerShardType::None => {}
        };

        add_tls_options(&mut args, tls_options);
        monger.start_mongod(args, "4.2.0", ChildType::Fork)?;
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

    let client = Client::with_options(
        ClientOptions::builder()
            .hosts(hosts)
            .repl_set_name(set_name.to_string())
            .tls_options(tls_options.map(|opts| opts.clone().into()))
            .build(),
    )?;

    configure_repl_set(&client, config)?;

    Ok(client)
}
