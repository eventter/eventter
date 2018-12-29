<?php
use Bunny\Message;
use Bunny\Channel;
use Bunny\Client;

$channel->consume(function (Message $message, Channel $channel, Client $client){

    // ... process message ...

    $channel->ack($message);

}, "my-queue");

$client->run();
