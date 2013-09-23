<?php

return array(
    'gearman_manager' => array(
        'host' => '127.0.0.1:4730', // "127.0.0.1:4730,127.0.0.1:4731,127.0.0.1:4732"
        'timeout' =>5000,
        // 10 workers will do all jobs
        'count'=>10,
        // Each job will have minimum 1 worker
        // that does only that job
        'dedicated_count' => 1,
        // Workers will only live for 1 hour
        'max_worker_lifetime'=>3600,
        // Reload workers as new code is available
        'auto_update'=>0,
        'max_runs_per_worker' => 20,
        'user' => 'www-data',
        'daemonize' => true,
        'log_file' =>'data/logs/gearman_manager.log',
        'worker_restart_splay' => 300,
        
        // Timeout n seconds for all jobs before work is reissued to another worker
        'timeout' => 300,
        'workers' => array(
            'do-stuff' => array(
                'worker' =>  'Application\Worker\DoStuff',
                'dedicated_count'   => 3,
                'dedicated_only'    => 1,
                'count'             => 1,
                'timeout'           => 30,
                
            )
        ),
    )
    ,
    'service_manager' => array(
        'invokables' => array(
//            'Application\Worker\DoStuff' => 'Application\Worker\DoStuff'
        )
    )
);
