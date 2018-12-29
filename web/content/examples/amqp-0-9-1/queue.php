<?php
$channel->queueDeclare(
    "my-queue", // name
    false,      // passive
    true,       // durable
    false,      // exclusive
    false,      // auto-delete
    false,      // no-wait
    [           // arguments
        "size" => 1024,
    ]
);
