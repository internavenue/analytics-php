<?php

class Segment_Consumer_NSQ extends Segment_Consumer {

  private $addresses;
  protected $type = "NSQ";

  /**
   * The file consumer publish track and identify calls to NSQ.
   * @param string $secret
   * @param array  $options
   *     array of string "addresses" NSQ addresses to publish message
   */
  public function __construct($secret, $options = array()) {
    parent::__construct($secret, $options);
    if (isset($options['addresses'])) {
        $this->addresses = $options['addresses'];
    } else {
        $this->addresses = array();
    }
  }

  /**
   * Tracks a user action
   * 
   * @param  array $message
   * @return [boolean] whether the track call succeeded
   */
  public function track(array $message) {
    return $this->write($message);
  }

  /**
   * Tags traits about the user.
   * 
   * @param  array $message
   * @return [boolean] whether the identify call succeeded
   */
  public function identify(array $message) {
    return $this->write($message);
  }

  /**
   * Tags traits about the group.
   * 
   * @param  array $message
   * @return [boolean] whether the group call succeeded
   */
  public function group(array $message) {
    return $this->write($message);
  }

  /**
   * Tracks a page view.
   * 
   * @param  array $message
   * @return [boolean] whether the page call succeeded
   */
  public function page(array $message) {
    return $this->write($message);
  }

  /**
   * Tracks a screen view.
   * 
   * @param  array $message
   * @return [boolean] whether the screen call succeeded
   */
  public function screen(array $message) {
    return $this->write($message);
  }

  /**
   * Aliases from one user id to another
   * 
   * @param  array $message
   * @return boolean whether the alias call succeeded
   */
  public function alias(array $message) {
    return $this->write($message);
  }

  /**
   * Writes the API call to the list of given NSQ addresses
   * @param  [array]   $body post body content.
   * @return [boolean] whether if any of the requests succeeded
   */
  private function write($body) {
    $ch = array();
    foreach ($this->addresses as $idx => $addr) {
        $ch[$idx] = curl_init("$addr/pub?topic=$topic");
        curl_setopt($ch[$idx], CURLOPT_POSTFIELDS, $message);
        curl_setopt($ch[$idx], CURLOPT_HTTPHEADER, array('Content-Type:application/json'));
        curl_setopt($ch[$idx], CURLOPT_RETURNTRANSFER, true);
    }
    $mh = curl_multi_init();
    foreach ($ch as $c) {
        curl_multi_add_handle($mh, $c);
    }
    $active = null;
    //execute the handles
    do {
        $mrc = curl_multi_exec($mh, $active);
    } while ($mrc == CURLM_CALL_MULTI_PERFORM);

    while ($active && $mrc == CURLM_OK) {
        if (curl_multi_select($mh) != -1) {
            do {
                $mrc = curl_multi_exec($mh, $active);
            } while ($mrc == CURLM_CALL_MULTI_PERFORM);
        }
    }
    foreach ($ch as $c) {
        $httpCode = curl_getinfo($c, CURLINFO_HTTP_CODE);
        curl_multi_remove_handle($mh, $c);
        if ($httpCode == 200) {
            return true;
        }
    }
    curl_multi_close($mh);
    return false;
  }
}
