<?php

namespace ZfGearmanManager;

class Module {

    public function getAutoloaderConfig() {
        return array(
            'Zend\Loader\StandardAutoloader' => array(
                'namespaces' => array(
                    __NAMESPACE__ => __DIR__ . '/src/' . __NAMESPACE__,
                ),
            ),
        );
    }

    public function getServiceConfig() {
        return array(
            'factories' => array(
                'GearmanClient' => function ($sm) {
                    if (!extension_loaded('gearman')) {
                        return;
                    }

                    $config = $sm->get('config');
                    try {
                        $client = new \GearmanClient();
                        $client->addServers($config['gearman']['servers']);
                        return $client;
                    } catch (\Exception $e) {
                        $sm->get('Zend\Log')->emerg('Gearman client cant connect to server! Message: ' . $e->getMessage());
                    }
                },
                'GearmanWorker' => function($sm) {
                    $config = $sm->get('config');
                    $worker = new \GearmanWorker();
                    $worker->setOptions(GEARMAN_WORKER_NON_BLOCKING);
                    $worker->setTimeout($config['gearman']['timeout']);
                    $worker->addServers($config['gearman']['servers']);
                    return $worker;
                },
                'ZfGearmanPeclManager' => function ($sm) {
                    $manager = new ZfGearmanPeclManager();
                    $manager->setServiceLocator($sm);

                    return $manager;
                }
            )
        );
    }

}
