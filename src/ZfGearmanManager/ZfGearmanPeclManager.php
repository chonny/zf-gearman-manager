<?php

namespace ZfGearmanManager;

use GearmanManager\Bridge\GearmanPeclManager;
use Zend\ServiceManager\ServiceLocatorAwareInterface;
use Zend\ServiceManager\ServiceLocatorInterface;
use ZfGearmanManager\Worker\WorkerInterface;
use GearmanManager\GearmanManager;

class ZfGearmanPeclManager extends GearmanPeclManager implements ServiceLocatorAwareInterface
{
    /**
     * Service Locator
     *
     * @var Zend\ServiceManager\ServiceLocatorInterface
     */
    protected $sm;

    /**
     * Overrides GearmanManager's constructor to remove all of the
     * 'start up' functionality (which is now in the start() method)
     *
     * This is mainly to allow other depdendencies to be passed to
     * the instance (i.e. in the service locator) before it starts
     * doing it's stuff
     */
    public function __construct()
    {
        if(!function_exists("posix_kill")){
            $this->show_help("The function posix_kill was not found. Please ensure POSIX functions are installed");
        }

        if(!function_exists("pcntl_fork")){
            $this->show_help("The function pcntl_fork was not found. Please ensure Process Control functions are installed");
        }
    }

    /**
     * Starts up the manager
     *
     * @return void
     */
    public function start()
    {
        $this->pid = getmypid();

        // Parse command line options. Loads the config file as well
        $this->getopt();
        
        // Register signal listeners
        $this->register_ticks();
        
        // Load up the workers
        $this->load_workers();
        if (empty($this->functions)){
            $this->log("No workers found");
            posix_kill($this->pid, SIGUSR1);
            exit();
        }

        // Validate workers in the helper process
        $this->fork_me("validate_workers");

        $this->log("Started with pid $this->pid", self::LOG_LEVEL_PROC_INFO);

        // Start the initial workers and set up a running environment
        $this->bootstrap();

        $this->process_loop();

        // Kill the helper if it is running
        if (isset($this->helper_pid)){
            posix_kill($this->helper_pid, SIGKILL);
        }

        $this->log("Exiting");
    }
    /**
     * Parses the config file
     *
     * @param   string    $file     The config file. Just pass so we don't have
     *                              to keep it around in a var
     */
    protected function parse_config($file) {
        $this->log("Loading configuration from $file");

        if (substr($file, -4) == ".php"){

            require $file;

        } elseif(substr($file, -4) == ".ini"){

            $gearman_config = parse_ini_file($file, true);

        }
        $config = $this->getServiceLocator()->get('config');

        if (empty($gearman_config) && !isset($config['gearman_manager'])){
            $this->show_help("No configuration found in $file and application config");
        }
        if(isset($config['gearman_manager']) && isset($gearman_config['gearman_manager'])){
            $conf = array_merge($config['gearman_manager'],$gearman_config['gearman_manager']);
        }else{
            $conf = $config['gearman_manager'];
        }

        
        $this->config = $conf;
        $this->config['functions'] = array();

        foreach($conf['workers'] as $function=>$data){
                $this->config['functions'][$function] = $data;

        }

    }
     /**
     * Parses the command line options
     *
     */
    protected function getopt() {
        $config = $this->getServiceLocator()->get('config');
        $opts = getopt("ac:dD:h:Hl:o:p:P:u:v::w:r:x:Z");

        if(isset($opts["H"])){
            $this->show_help();
        }
        if(!isset($config['gearman_manager'])){
            $this->show_help("Config file should contains gearman_manager section.");
        }

        /**
         * parse the config file
         */
        $this->parse_config(null);

        /**
         * command line opts always override config file
         */
        if (isset($opts['P'])) {
            $this->config['pid_file'] = $opts['P'];
        }

        if(isset($opts["l"])){
            $this->config['log_file'] = $opts["l"];
        }

        if (isset($opts['a'])) {
            $this->config['auto_update'] = 1;
        }

        if (isset($opts['w'])) {
            $this->config['worker_dir'] = $opts['w'];
        }

        if (isset($opts['x'])) {
            $this->config['max_worker_lifetime'] = (int)$opts['x'];
        }

        if (isset($opts['r'])) {
            $this->config['max_runs_per_worker'] = (int)$opts['r'];
        }

        if (isset($opts['D'])) {
            $this->config['count'] = (int)$opts['D'];
        }

        if (isset($opts['t'])) {
            $this->config['timeout'] = $opts['t'];
        }

        if (isset($opts['h'])) {
            $this->config['host'] = $opts['h'];
        }

        if (isset($opts['p'])) {
            $this->prefix = $opts['p'];
        } elseif(!empty($this->config['prefix'])) {
            $this->prefix = $this->config['prefix'];
        }

        if(isset($opts['u'])){
            $this->user = $opts['u'];
        } elseif(isset($this->config["user"])){
            $this->user = $this->config["user"];
        }

        /**
         * If we want to daemonize, fork here and exit
         */
        if(isset($opts["d"]) || (isset($this->config['daemonize']) && $this->config['daemonize'])){
            $pid = pcntl_fork();
            if($pid>0){
                $this->isparent = false;
                exit();
            }
            $this->pid = getmypid();
            posix_setsid();
        }

        if(!empty($this->config['pid_file'])){
            $fp = @fopen($this->config['pid_file'], "w");
            if($fp){
                fwrite($fp, $this->pid);
                fclose($fp);
            } else {
                $this->show_help("Unable to write PID to {$this->config['pid_file']}");
            }
            $this->pid_file = $this->config['pid_file'];
        }

        if(!empty($this->config['log_file'])){
            if($this->config['log_file'] === 'syslog'){
                $this->log_syslog = true;
            } else {
                $this->log_file = $this->config['log_file'];
                $this->open_log_file();
            }
        }

        if(isset($opts["v"])){
            switch($opts["v"]){
                case false:
                    $this->verbose = GearmanManager::LOG_LEVEL_INFO;
                    break;
                case "v":
                    $this->verbose = GearmanManager::LOG_LEVEL_PROC_INFO;
                    break;
                case "vv":
                    $this->verbose = GearmanManager::LOG_LEVEL_WORKER_INFO;
                    break;
                case "vvv":
                    $this->verbose = GearmanManager::LOG_LEVEL_DEBUG;
                    break;
                case "vvvv":
                default:
                    $this->verbose = GearmanManager::LOG_LEVEL_CRAZY;
                    break;
            }
        }

        if($this->user) {
            $user = posix_getpwnam($this->user);
            if (!$user || !isset($user['uid'])) {
                $this->show_help("User ({$this->user}) not found.");
            }

            /**
             * Ensure new uid can read/write pid and log files
             */
            if(!empty($this->pid_file)){
                if(!chown($this->pid_file, $user['uid'])){
                    $this->log("Unable to chown PID file to {$this->user}", GearmanManager::LOG_LEVEL_PROC_INFO);
                }
            }
            if(!empty($this->log_file_handle)){
                if(!chown($this->log_file, $user['uid'])){
                    $this->log("Unable to chown log file to {$this->user}", GearmanManager::LOG_LEVEL_PROC_INFO);
                }
            }

            posix_setuid($user['uid']);
            if (posix_geteuid() != $user['uid']) {
                $this->show_help("Unable to change user to {$this->user} (UID: {$user['uid']}).");
            }
            $this->log("User set to {$this->user}", GearmanManager::LOG_LEVEL_PROC_INFO);
        }

        if(!empty($this->config['auto_update'])){
            $this->check_code = true;
        }

//        if(!empty($this->config['worker_dir'])){
//            $this->worker_dir = $this->config['worker_dir'];
//        } else {
//            $this->worker_dir = "./workers";
//        }
//
//        $dirs = explode(",", $this->worker_dir);
//        foreach($dirs as &$dir){
//            $dir = trim($dir);
//            if(!file_exists($dir)){
//                $this->show_help("Worker dir ".$dir." not found");
//            }
//        }
//        unset($dir);

        if(isset($this->config['max_worker_lifetime']) && (int)$this->config['max_worker_lifetime'] > 0){
            $this->max_run_time = (int)$this->config['max_worker_lifetime'];
        }

        if(isset($this->config['worker_restart_splay']) && (int)$this->config['worker_restart_splay'] > 0){
            $this->worker_restart_splay = (int)$this->config['worker_restart_splay'];
        }

        if(isset($this->config['count']) && (int)$this->config['count'] > 0){
            $this->do_all_count = (int)$this->config['count'];
        }

        if(!empty($this->config['host'])){
            if(!is_array($this->config['host'])){
                $this->servers = explode(",", $this->config['host']);
            } else {
                $this->servers = $this->config['host'];
            }
        } else {
            $this->servers = array("127.0.0.1");
        }

        if (!empty($this->config['include']) && $this->config['include'] != "*") {
            $this->config['include'] = explode(",", $this->config['include']);
        } else {
            $this->config['include'] = array();
        }

        if (!empty($this->config['exclude'])) {
            $this->config['exclude'] = explode(",", $this->config['exclude']);
        } else {
            $this->config['exclude'] = array();
        }

        /**
         * Debug option to dump the config and exit
         */
        if(isset($opts["Z"])){
            print_r($this->config);
            exit();
        }

    }
    /**
     * Set service locator
     *
     * @param ServiceLocatorInterface $serviceLocator
     */
    public function setServiceLocator(ServiceLocatorInterface $serviceLocator)
    {
        $this->sm = $serviceLocator;

        return $this;
    }

    /**
     * Get service locator
     *
     * @return ServiceLocatorInterface
     */
    public function getServiceLocator()
    {
        return $this->sm;
    }

    /**
     * Helper function to load and filter worker files
     *
     * @return void
     */
    protected function load_workers()
    {
        $config = $this->getServiceLocator()->get('Config');
        if (!isset($config['gearman_manager']['workers']) || empty($config['gearman_manager']['workers'])) {
            return;
        }

        $workers = $config['gearman_manager']['workers'];

        $this->log('Loading '.count($workers) .' worker(s) from config');

        $this->functions = array();

        foreach ($workers as $function => $w_options) {

            // TODO include/exclude functionality from GearmanManager

            if (!isset($this->functions[$function])){
                $this->functions[$function] = $w_options;
            }

            if (!empty($this->config['functions'][$function]['dedicated_only'])){
                if(empty($this->config['functions'][$function]['dedicated_count'])){
                    $this->log("Invalid configuration for dedicated_count for function $function.", self::LOG_LEVEL_PROC_INFO);
                    exit();
                }

                $this->functions[$function]['dedicated_only'] = true;
                $this->functions[$function]["count"] = $this->config['functions'][$function]['dedicated_count'];

            } else {

                $min_count = max($this->do_all_count, 1);
                if(!empty($this->config['functions'][$function]['count'])){
                    $min_count = max($this->config['functions'][$function]['count'], $this->do_all_count);
                }

                if(!empty($this->config['functions'][$function]['dedicated_count'])){
                    $ded_count = $this->do_all_count + $this->config['functions'][$function]['dedicated_count'];
                } elseif(!empty($this->config["dedicated_count"])){
                    $ded_count = $this->do_all_count + $this->config["dedicated_count"];
                } else {
                    $ded_count = $min_count;
                }

                $this->functions[$function]["count"] = max($min_count, $ded_count);

            }

            /**
             * Note about priority. This exploits an undocumented feature
             * of the gearman daemon. This will only work as long as the
             * current behavior of the daemon remains the same. It is not
             * a defined part fo the protocol.
             */
            if(!empty($this->config['functions'][$function]['priority'])){
                $priority = max(min(
                    $this->config['functions'][$function]['priority'],
                    self::MAX_PRIORITY), self::MIN_PRIORITY);
            } else {
                $priority = 0;
            }

            $this->functions[$function]['priority'] = $priority;
        }
    }

    /**
     * Starts a worker for the PECL library
     *
     * Overrides the function from the parent class to remove the error suppression
     * from worker calls
     *
     * @param   array   $worker_list    List of worker functions to add
     * @param   array   $timeouts       list of worker timeouts to pass to server
     * @return  void
     *
     */
    protected function start_lib_worker($worker_list, $timeouts = array()) {
        $thisWorker = $this->getServiceLocator()->get('GearmanWorker');
//        $thisWorker = new \GearmanWorker();
//
//        $thisWorker->addOptions(GEARMAN_WORKER_NON_BLOCKING);
//
//        $thisWorker->setTimeout(5000);
        foreach($this->servers as $s){
            $this->log("Adding server $s", self::LOG_LEVEL_WORKER_INFO);
            $thisWorker->addServers($s);
        }

        foreach($worker_list as $w){
            $timeout = (isset($timeouts[$w]) ? $timeouts[$w] : null);
            $message = "Adding job $w";
            if($timeout){
                $message.= "; timeout: $timeout";
            }
            $this->log($message, self::LOG_LEVEL_WORKER_INFO);
            $thisWorker->addFunction($w, array($this, "do_job"), $this, $timeout);
        }

        $start = time();

        while(!$this->stop_work){

            if($thisWorker->work() ||
               $thisWorker->returnCode() == GEARMAN_IO_WAIT ||
               $thisWorker->returnCode() == GEARMAN_NO_JOBS) {

                if ($thisWorker->returnCode() == GEARMAN_SUCCESS) continue;

                if (!@$thisWorker->wait()){
                    if ($thisWorker->returnCode() == GEARMAN_NO_ACTIVE_FDS){
                        sleep(5);
                    }
                }

            }

            /**
             * Check the running time of the current child. If it has
             * been too long, stop working.
             */
            if($this->max_run_time > 0 && time() - $start > $this->max_run_time) {
                $this->log("Been running too long, exiting", self::LOG_LEVEL_WORKER_INFO);
                $this->stop_work = true;
            }

            if(!empty($this->config["max_runs_per_worker"]) && $this->job_execution_count >= $this->config["max_runs_per_worker"]) {
                $this->log("Ran $this->job_execution_count jobs which is over the maximum({$this->config['max_runs_per_worker']}), exiting", self::LOG_LEVEL_WORKER_INFO);
                $this->stop_work = true;
            }

        }

        $thisWorker->unregisterAll();


    }

    /**
     * Wrapper function handler for all registered functions
     * This allows us to do some nice logging when jobs are started/finished
     */
    public function do_job($job) {

        static $objects;

        if($objects===null) $objects = array();

        $w = $job->workload();

        $h = $job->handle();

        $job_name = $job->functionName();

        if($this->prefix){
            $func = $this->prefix.$job_name;
        } else {
            $func = $job_name;
        }

        $fqcn = $this->getWorkerFqcn($job_name);
        if ($fqcn) {
            $this->log("Creating a $func object", self::LOG_LEVEL_WORKER_INFO);
            if( $this->getServiceLocator()->has($fqcn)){
                $objects[$job_name] = $this->getServiceLocator()->get($fqcn);
            }else{
                $this->log("Service manager can't find the worker definitiion $fqcn");
                return;
            }

            if (!$objects[$job_name] || !is_object($objects[$job_name])) {
                $this->log("Invalid worker class registered for $job_name (not an object?)");
                return;
            }

            if (!($objects[$job_name] instanceof WorkerInterface)) {
                $this->log("Worker class ".get_class($objects[$job_name])." registered for $job_name must implement ZfGearmanManager\Worker\WorkerInterface");
                return;
            }

        } else {
            $this->log("Function $func not found");
            return;
        }

        $this->log("($h) Starting Job: $job_name", self::LOG_LEVEL_WORKER_INFO);

        $this->log("($h) Workload: $w", self::LOG_LEVEL_DEBUG);

        $log = array();

        /**
         * Run the real function here
         */
        if(isset($objects[$job_name])){
            $this->log("($h) Calling object for $job_name.", self::LOG_LEVEL_DEBUG);
            $result = $objects[$job_name]->run($job, $log);
            unset($objects[$job_name]);

        } elseif(function_exists($func)) {
            $this->log("($h) Calling function for $job_name.", self::LOG_LEVEL_DEBUG);
            $result = $func($job, $log);
        } else {
            $this->log("($h) FAILED to find a function or class for $job_name.", self::LOG_LEVEL_INFO);
        }

        if(!empty($log)){
            foreach($log as $l){

                if(!is_scalar($l)){
                    $l = explode("\n", trim(print_r($l, true)));
                } elseif(strlen($l) > 256){
                    $l = substr($l, 0, 256)."...(truncated)";
                }

                if(is_array($l)){
                    foreach($l as $ln){
                        $this->log("($h) $ln", self::LOG_LEVEL_WORKER_INFO);
                    }
                } else {
                    $this->log("($h) $l", self::LOG_LEVEL_WORKER_INFO);
                }

            }
        }

        $result_log = $result;

        if(!is_scalar($result_log)){
            $result_log = explode("\n", trim(print_r($result_log, true)));
        } elseif(strlen($result_log) > 256){
            $result_log = substr($result_log, 0, 256)."...(truncated)";
        }

        if(is_array($result_log)){
            foreach($result_log as $ln){
                $this->log("($h) $ln", self::LOG_LEVEL_DEBUG);
            }
        } else {
            $this->log("($h) $result_log", self::LOG_LEVEL_DEBUG);
        }

        /**
         * Workaround for PECL bug #17114
         * http://pecl.php.net/bugs/bug.php?id=17114
         */
        $type = gettype($result);
        settype($result, $type);


        $this->job_execution_count++;

        return $result;

    }

    /**
     * Returns the fully qualified class name (from the module config)
     * for $func, or false if it doesn't exist
     *
     * @param  string $func
     * @return string|false
     */
    protected function getWorkerFqcn($func)
    {
        $config = $this->getServiceLocator()->get('Config');
        if (!isset($config['gearman_manager']) || !isset($config['gearman_manager']['workers'][$func])) {
            return false;
        }

        return $config['gearman_manager']['workers'][$func]['worker'];
    }

    /**
     * Validates the PECL compatible worker files/functions
     */
    protected function validate_lib_workers()
    {
        foreach ($this->functions as $func => $props){
            $real_func = $this->prefix.$func;

            if (!$this->getWorkerFqcn($real_func)) {
                $this->log("Function $real_func not found");
                posix_kill($this->pid, SIGUSR2);
                exit();
            }
        }
    }
}
