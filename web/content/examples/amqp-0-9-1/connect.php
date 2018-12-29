<?php
use Bunny\Client;

$conn = new Client([
    "host" => "127.0.0.1",
    "port" => 16001,
    "vhost" => "default",
]);
$conn->connect();

// ... work with connection ...
