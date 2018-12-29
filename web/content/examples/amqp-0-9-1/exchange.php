<?php
$channel->exchangeDeclare(
    "my-exchange", // name
    "fanout",      // type
    false,         // passive
    true,          // durable
    false,         // auto-delete
    false,         // internal
    false,         // no-wait
    [              // arguments
        "shards"             => 1,
        "replication-factor" => 3,
        "retention"          => "24h",
    ]
);
