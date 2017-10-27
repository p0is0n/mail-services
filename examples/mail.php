<?php

require_once(realpath(dirname(__FILE__) . '/../') . '/api/MailServices.php');

$mailServices = new MailServices(array(
	'host' => '127.0.0.1',
	'port' => 6132,
));

// Reset all data
$mailServices -> resetData();
$mailServices -> bufferTo(true);

// Add message
$mailServices -> addMessage(array(
	'html' => 'Привет {:name:}! test {:testPart1:} html <img src="http://www.flip.kz/img/logo.png"> {:testPart2:}',
	#'text' => 'Привет {:name:}! test {:testPart1:} text {:testPart2:}',

	'subject' => 'Ааааааааа!!! test subject test subject test subject test subject test subject test subject',
	'sender' => (array(
		'name' => 'Test from, рус.',
		'email' => 'shop@flip.kz'
	)),
	'headers' => (array(
		'X-Mailru-Msgtype' => 'test1',
	)),
));

// Add group
var_dump( $mailServices -> addGroup(array(
	'id' => 26,
	'status' => MailServices::GROUP_STATUS_ACTIVE,
)) );

/*
$rows = (array(
	array('i' => 3, 'priority' => 1),
	array('i' => 1, 'priority' => 9),
	array('i' => 4, 'priority' => 0),
	array('i' => 2, 'priority' => 2),
));

foreach($rows as $row) {
	$mailServices -> addTo(array(
		'name' => 'Name' . $row['i'] . ' рус.',
		'email' => 'poisonoff' . $row['i'] . '@gmail.com',
		'priority' => $row['priority'],
		'parts' => (array(
			'testPart1' => 1,
			'testPart2' => 'ok' . $row['i'],
		)),
		// 'delay' => (5 + $row['i']),
	));
}

// Flush all
$mailServices -> bufferToFlush();
*/


for ($j = 0; $j <= 0; $j++) {
	/*// Add message
	$mailServices -> addMessage(array(
		'html' => 'Привет {:name:}! test {:testPart1:} html <img src="http://www.flip.kz/img/logo.png"> {:testPart2:}',
		'text' => 'Привет {:name:}! test {:testPart1:} text {:testPart2:}',

		'subject' => 'Ааааааааа!!! test subject test subject test subject test subject test subject test subject',
		'sender' => (array(
			'name' => 'Test from, рус.',
			'email' => 'shop@flip.kz'
		)),
	));*/

	// Add to
	for ($i = 1; $i <= 1; $i++) {
		$mailServices -> addTo(array(
			'name' => 'Name' . $i . ' рус.',
			'email' => 'pавпвапnoff@gmail.com',
			'priority' => 5,
			'parts' => (array(
				'testPart1' => 1,
				'testPart2' => 'ok' . $i,
			)),
			//'delay' => 10,
		));

		if ($i % 1000 === 0) {
			$mailServices -> bufferToFlush();
		}
	}

	// Flush all
	$mailServices -> bufferToFlush();
}

// Group stats
var_dump( $mailServices -> getGroup(26) );
//var_dump( $mailServices -> deleteMessage(20) );
