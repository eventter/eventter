<?php
$channel->queueBind(
    "my-queue",    // queue
    "my-exchange", // exchange
    "",            // routing key
    false,         // no-wait
    []             // binding arguments
);
