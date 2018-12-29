<?php
$channel->publish(
    "hello, exchange", // message body
    [],                // message properties / headers
    "my-exchange",     // exchange = empty string means default exchange
    ""                 // routing key = because exchange has type fanout, all messages are passed to bound queues
);
