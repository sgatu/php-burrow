<?php

namespace Burrow;
/**
*   Simple curl based client for burrow API
*
*   @author Stefan Gatu
*/
class Client
{
    /**
    * @var string $host The burrow hostname
    */
    private $host;

    /**
    * @var int $port Burrow advertising port
    */
    private $port;


    private $options = [
        "timeout" => 5,
        "timeout_ms" => 0
    ];
    /**
     * @param string $host Burrow host
     * @param int $port The burrow port, by default 8000
     * @param mixed[] $options Additional options, like timeout
     */
    public function __construct($host, $port = 8000, $options = [])
    {
        if(!ctype_digit((string) $port)){
            throw new InvalidArgumentException("The port must be numeric.");
        }
        $this->options = array_merge($this->options, $options);
        $this->host = str_ireplace("https://", "", str_ireplace("http://","",$host));
        $this->port = $port;
    }

    /**
    *  Makes an http request and returns an json decoded array
    *
    *  @param string $endpoint The uri where to make the request
    *  @param string $method The http method(GET,POST,PUT...)
    *  @param string|string[] $data Post data to send in the request
    *  @param string|string[] $headers Additional headers for the request
    */
    private function _request($endpoint, $method = "GET", $data = null, $headers = [])
    {
        if(!in_array(strtoupper($method), ["GET","HEAD","POST","OPTIONS","PUT","PATCH","DELETE"])){
            throw new InvalidArgumentException("Invalid method parameter. Got: ".$method.". Expected GET, POST, HEAD, OPTIONS, PUT, PATCH or DELETE.");
        }
        $method = strtoupper($method);
        $ch = curl_init("http://".$this->host.":".$this->port.$endpoint);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        if(is_array($headers)){
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        }
        else if(is_string($headers)){
            curl_setopt($ch, CURLOPT_HTTPHEADER, [$headers]);
        }
        if($data != null && is_string($data)){
            curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
        }
        else if(is_array($data)){
            curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($data));
        }
        curl_setopt($ch, CURLOPT_TIMEOUT, intval($this->options["timeout"]));
        curl_setopt($ch, CURLOPT_TIMEOUT_MS, intval($this->options["timeout_ms"]));

        $server_output = curl_exec ($ch);
        curl_close ($ch);
        return json_decode($server_output, true);
    }
    /**
    *  Returns the list of the clusters
    *
    */
    public function getClusters()
    {
        $result = $this->_request("/v2/kafka");
        if(!$result || $result['error'] != false){
            return false;
        }
        return $result["clusters"];
    }
    /**
    *   Returns cluster info
    *
    *   @param string $cluster The cluster name
    */
    public function getClusterInfo($cluster)
    {
        $result = $this->_request("/v2/kafka/$cluster");
        if(!$result || $result['error'] != false){
            return false;
        }
        return $result["cluster"];
    }
    /**
    * Returns a list with the zookeepers servers and their ports
    *
    *  @param string $cluster The cluster name
    */
    public function getZookeeperServers($cluster)
    {
        $info = $this->getClusterInfo($cluster);
        if(!$info) return false;
        $zookeepers = [];
        foreach($info["zookeepers"] as $server){
            $parts = explode(":", $server);
            $zookeepers[] = ["server"=>trim($parts[0],"[]"),"port"=>intval($parts[1])];
        }
        return $zookeepers;
    }

    /**
    * Returns a list with the brokers and their ports
    *
    *  @param string $cluster The cluster name
    */
    public function getBrokers($cluster)
    {
        $info = $this->getClusterInfo($cluster);
        if(!$info) return false;
        $brokers = [];
        foreach($info["brokers"] as $server){
            $parts = explode(":", $server);
            $brokers[] = ["server"=>trim($parts[0],"[]"),"port"=>intval($part[1])];
        }
        return $brokers;
    }

    /**
    * Returns a list with all the consumers in the cluster
    *
    *  @param string $cluster The cluster name
    */
    public function getConsumers($cluster)
    {
        $result = $this->_request("/v2/kafka/$cluster/consumer");
        if(!$result || $result['error'] != false){
            return false;
        }
        return $result["consumers"];
    }

    /**
    * Returns a list with all the consumers in the cluster
    *
    *  @param string $cluster The cluster name
    */
    public function getConsumerTopics($cluster, $consumer)
    {
        $result = $this->_request("/v2/kafka/$cluster/consumer/$consumer/topic");
        if(!$result || $result['error']){
            return false;
        }
        return $result['topics'];
    }
    /**
    * Returns a list of the offsets commited by the consumer by partition of the specified topic
    *
    *  @param string $cluster The cluster name
    *  @param string $consumer The consumer name
    *  @param string $topic The topic
    */
    public function getConsumerOffsets($cluster, $consumer, $topic)
    {
        $result = $this->_request("/v2/kafka/$cluster/consumer/$consumer/topic/$topic");
        if(!$result || $result['error']){
            return false;
        }
        return $result['offsets'];
    }
    /**
    *   Returns a list with all the topics in the cluster
    *
    *   @param string $cluster The cluster name
    */
    public function getTopics($cluster)
    {
        $result = $this->_request("/v2/kafka/$cluster/topic");
        if(!$result || $result['error']){
            return false;
        }
        return $result['topics'];
    }

    /**
    *   Returns a list all the offsets of the specified topic
    *
    *   @param string $cluster The cluster name
    *   @param string $topic The topic
    */
    public function getTopicOffsets($cluster, $topic)
    {
        $result = $this->_request("/v2/kafka/$cluster/topic/$topic");
        if(!$result || $result['error']){
            return false;
        }
        return $result['offsets'];
    }

    public function getConsumerStatus($cluster, $consumer){
        $result = $this->_request("/v2/kafka/$cluster/consumer/$consumer/lag");
        if(!$result || $result['error']){
            return false;
        }
        return $result['status'];
    }
    /**
    *   Return a list with all the topics and their partitions, specifing the last lag, last offset commited and when
    *
    *   @param string $cluster The cluster name
    *   @param string $consumer The consumer name
    */
    public function getConsumerLag($cluster, $consumer){
        $result = $this->getConsumerStatus($cluster, $consumer);
        if(!$result){
            return false;
        }
        $status = [];
        foreach($result['partitions'] as $k => $v){
            if(!isset($status[$v['topic']])){
                $status[$v['topic']] = [];
            }
            if(!isset($status[$v['topic']][$v['partition']])){
                $status[$v['topic']][$v['partition']] = [
                    "status"=>$v['status'],
                    "lag"=>$v['end']['lag'],
                    "timestamp"=>$v['end']['timestamp'],
                    "offset"=>$v['end']['offset']
                ];
            }
        }
        return $status;
    }



    /**
    *   Deletes as consumer group from the cluster, if the consumer is still committing offsets it will reapear in the list
    *
    *   @param string $cluster The cluster name
    *   @param string $consumer The consumer name
    */
    public function deleteConsumer($cluster, $consumer)
    {
        $result = $this->_request("/v2/kafka/$cluster/consumer/$consumer", "DELETE");
        if(!$result || $result["error"] != false){
            return false;
        }
        return true;
    }
}
