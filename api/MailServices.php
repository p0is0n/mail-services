<?php

class MailServices {

	protected $connection = null;
	protected $data = null;

	// Defaults params
	protected $defaults = array(
		'host' => '127.0.0.1',
		'port' => 6132,
		'timeout' => 10,
	);

	protected $bufferTo = false;

	const GROUP_STATUS_ACTIVE = 1;
	const GROUP_STATUS_PAUSED = 2;
	const GROUP_STATUS_INACTIVE = 3;

	public function __construct(Array $params = array()) {
		if (isset($params['host'])) {
			$host = $params['host'];
		}
		else {
			$host = $this -> defaults['host'];
		}

		if (isset($params['port'])) {
			$port = $params['port'];
		}
		else {
			$port = $this -> defaults['port'];
		}

		$timeout = $this -> defaults['timeout'];
		$success = null;

		if (! ($this -> connection = stream_socket_client('tcp://' . $host . ':' . $port . '/', $errno, $errstr, $timeout, STREAM_CLIENT_CONNECT))) {
			throw new Exception('Cannot connect to "' . $host . ':' . $port . '", reason ' . $errstr);
		}
	}

	protected function send(Array $data) {
		if (!! ($this -> connection)) {
			$data = json_encode($data);

			if (!! ($data)) {
				return fwrite($this -> connection, pack('N', strlen($data)) . $data);
			}
			else {
				// Fail
				return false;
			}
		}
		else {
			throw new Exception('No connection on mail-services');
		}
	}

	protected function read() {
		if (!! ($this -> connection)) {
			$length = fread($this -> connection, 4);
			$length = array_pop(unpack('N', $length));

			$buffer = '';

			while ($length > strlen($buffer)) {
				if (false !== ($data = fread($this -> connection, $length))) {
					$buffer .= $data;
				}
				else {
					// Fail
					return false;
				}
			}

			if (! empty($buffer)) {
				$data = json_decode($buffer, true);

				if (!! ($data)) {
					return $data;
				}
				else {
					// Fail
					return false;
				}
			}
			else {
				// Fail
				return false;
			}
		}
		else {
			throw new Exception('No connection on mail-services');
		}
	}

	protected function sendAndRead(Array $data) {
		if (false !== $this -> send($data)) {
			if (false !== ($result = $this -> read())) {
				if (! empty($result['error'])) {
					throw new Exception('Mail-Services error "' . $result['error'] . '"');
				}

				return $result;
			}
			else {
				throw new Exception('Cannot read on mail-services');
			}
		}
		else {
			throw new Exception('Cannot send on mail-services');
		}
	}

	public function resetData() {
		if (null !== $this -> data) {
			// Reset all data
			$this -> data = null;
		}

		// Reset params
		$this -> bufferTo = false;
	}

	public function getGroups(Array $groups) {
		$result = $this -> sendAndRead(array(
			'command' => 'group',
			'ids' => $groups,
		));

		if (! empty($result)) {
			if (isset($result['groups'])) {
				return $result['groups'];
			}
			else {
				return null;
			}
		}

		throw new Exception('Mail-Services unknown error');
	}

	public function getGroup($group) {
		$result = $this -> sendAndRead(array(
			'command' => 'group',
			'ids' => $group,
		));

		if (! empty($result)) {
			if (isset($result['groups'][$group])) {
				return $result['groups'][$group];
			}
			else {
				return null;
			}
		}

		throw new Exception('Mail-Services unknown error');
	}

	public function addGroup(Array $data) {
		$result = $this -> sendAndRead(array(
			'command' => 'group',
			'group' => $data,
		));

		if (! empty($result)) {
			if (! empty($result['group']['id'])) {
				if (null === $this -> data) {
					$this -> data = array();
				}

				// Success
				return ($this -> data['group'] = $result['group']);
			}
		}

		throw new Exception('Mail-Services unknown error');
	}

	public function addMessage(Array $data) {
		$result = $this -> sendAndRead(array(
			'command' => 'message',
			'message' => $data,
		));

		if (! empty($result)) {
			if (! empty($result['message']['id'])) {
				if (null === $this -> data) {
					$this -> data = array();
				}

				// Success
				return ($this -> data['message'] = $result['message']);
			}
		}

		throw new Exception('Mail-Services unknown error');
	}

	public function addTo(Array $data) {
		if (false === $this -> bufferTo) {
			$result = $this -> sendAndRead(array(
				'command' => 'mail',
				'message' => (! empty($this -> data['message']) ? $this -> data['message'] : null),
				'to' => $data,
				'group' => (! empty($this -> data['group']) ? (is_array($this -> data['group']) ? $this -> data['group']['id'] : $this -> data['group']) : null),
			));

			if (! empty($result)) {
				if (! empty($result['counts'])) {
					// Success
					return $result['counts'];
				}
			}
		}
		else {
			// Buffer
			if (null === $this -> data) {
				$this -> data = array();
			}

			if (! isset($this -> data['to'])) {
				$this -> data['to'] = array();
			}

			// Push
			return (array_push($this -> data['to'], $data));
		}

		throw new Exception('Mail-Services unknown error');
	}

	public function delete($type, $id, $force = false) {
		$result = $this -> sendAndRead(array(
			'command' => 'delete',
			'force' => $force,
			$type => $id,
		));

		if (! empty($result)) {
			if (! empty($result[$type])) {
				// Success
				return $result[$type];
			}
		}

		throw new Exception('Mail-Services unknown error');
	}

	public function deleteGroup($id, $force = false) {
		return $this -> delete('group', $id, $force);
	}

	public function deleteMessage($id, $force = false) {
		return $this -> delete('message', $id, $force);
	}

	public function bufferTo($value) {
		$this -> bufferTo = !! $value;
	}

	public function bufferToFlush() {
		if ($this -> bufferToCount() > 0) {
			$result = $this -> send(array(
				'command' => 'mail',
				'message' => (! empty($this -> data['message']) ? $this -> data['message'] : null),
				'to' => $this -> data['to'],
				'group' => (! empty($this -> data['group']) ? (is_array($this -> data['group']) ? $this -> data['group']['id'] : $this -> data['group']) : null),
			));

			// Clean
			unset($this -> data['to']);

			if (false !== ($result = $this -> read())) {
				if (! empty($result['error'])) {
					throw new Exception('Mail-Services error "' . $result['error'] . '"');
				}

				if (! empty($result['counts'])) {
					// Success
					return $result['counts'];
				}
			}
			else {
				throw new Exception('Cannot read on mail-services');
			}

			throw new Exception('Mail-Services unknown error');
		}
	}

	public function bufferToCount() {
		if (! empty($this -> data['to'])) {
			return count($this -> data['to']);
		}

		// Default
		return 0;
	}

}
