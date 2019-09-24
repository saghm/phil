use std::time::Duration;

use phil_core::cluster::{Cluster, Topology};

fn main() {
    let cluster = Cluster::new(Topology::Single).unwrap();

    for _ in 0..5 {
        std::thread::sleep(Duration::from_secs(1));

        println!("cluster is up...");
    }

    cluster.shutdown();
}
