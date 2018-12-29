<?php
$channel->publish(
    "hello, queue", // message body
    [],             // message properties / headers
    "",             // exchange = empty string means default exchange
    "my-queue"      // routing key = queue name
);
