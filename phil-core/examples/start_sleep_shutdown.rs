use std::time::Duration;

use phil_core::cluster::{Cluster, ClusterOptions, Topology};

fn main() {
    let cluster = Cluster::new(
        ClusterOptions::builder()
            .topology(Topology::Single)
            .version_id("4.2")
            .build(),
    )
    .unwrap();

    for _ in 0..5 {
        std::thread::sleep(Duration::from_secs(1));

        println!("cluster is up...");
    }

    cluster.shutdown();
}
