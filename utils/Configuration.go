package utils

import "time"

//—————————————————————————————————————————————
// AWS SDK Settings
//—————————————————————————————————————————————
var ELB_ARN string = "arn:aws:elasticloadbalancing:us-east-1:786781699181:loadbalancer/net/sdcc-elb/6c29ee787b1a31df"
var AWS_CRED_PATH string = "/home/ec2-user/.aws/credentials"
var AUTOSCALING_NAME string = "sdcc-autoscaling"
var BUCKET_NAME string = "sdcc-cloud-resources"
var LB_DNS_NAME string = "sdcc-elb-6c29ee787b1a31df.elb.us-east-1.amazonaws.com"
var REGISTRY_IP string = "10.0.0.216"

//—————————————————————————————————————————————
// Time Settings
//—————————————————————————————————————————————
var RARELY_ACCESSED_TIME time.Duration = 30 * time.Minute           // Dopo quanto tempo un'entry viene migrata sul cloud
var RARELY_ACCESSED_CHECK_INTERVAL time.Duration = 15 * time.Minute // Ogni quanto controlliamo entry vecchie
var NODE_HEALTHY_TIME time.Duration = 30 * time.Second              // Tempo di attesa di un nodo prima che diventi healthy
var CHECK_TERMINATING_INTERVAL time.Duration = time.Minute          // Ogni quanto effettuare il controllo sulle istanze in terminazione
var START_CONSISTENCY_INTERVAL time.Duration = 10 * time.Minute     // Ogni quanto avviare il processo di scambio di aggiornamenti tra i nodi per la consistenza finale
var ACTIVITY_CACHE_FLUSH_INTERVAL time.Duration = 40 * time.Minute  // Ogni quanto flushare la cache sulle istanze in terminazione
var CHORD_FIX_INTERVAL time.Duration = 10 * time.Second             // Ogni quanto un nodo contatta i suoi vicini per aggiornare le Finger Table
var RR1_TIMEOUT time.Duration = 10 * time.Second                    // Tempo dopo il quale si considera perso un messaggio client-server e quindi si ritrasmette la richiesta di esec del servizio
var RR1_RETRIES = 5                                                 // Numero di ritrasmissioni RR1
var TEST_STEADY_TIME = 5 * time.Second                              // Tempo per inizializzare il workload nei test
var WAIT_SUCC_TIME = 10 * time.Second                               // Tempo che il nodo attende prima di provare a ricontattare il suo successore
var DIAL_RETRY = 3 * time.Second                                    // Tempo prima di effettuare un retry sulla Dial Http
var CHORD_STEADY_TIME = 20 * time.Second                            // Tempo necessario a chord per aggiornare tutte le finger table

//—————————————————————————————————————————————
// Port Settings
//—————————————————————————————————————————————
var HEARTBEAT_PORT string = ":8888"             // Porta su cui il nodo ascolta i segnali da load balancer e registry
var FILETR_REPLICATION_PORT string = ":7777"    // Porta su cui il nodo ascolta l'update mongo da altri nodi
var FILETR_RECONCILIATION_PORT string = ":6666" // Porta su cui il nodo ascolta l'update mongo per reconciliation da altri nodi
var FILETR_MIGRATION_PORT string = ":5555"      // Porta su cui il nodo ascolta l'update mongo per migration da altri nodi
var RPC_PORT string = ":80"                     // Porta su cui il nodo ascolta le chiamate RPC
var REGISTRY_PORT string = ":4444"              // Porta tramite cui il nodo instaura una connessione con il Service Registry
var CHORD_PORT string = ":3333"                 // Porta tramite cui il nodo riceve ed invia i messaggi necessari ad aggiornare la DHT Chord

//—————————————————————————————————————————————
// Update Messages
//—————————————————————————————————————————————
var RECON string = "reconciliation"
var REPLN string = "replication"
var MIGRN string = "migration"

//—————————————————————————————————————————————
// MongoDB Settings
//—————————————————————————————————————————————
var CSV string = ".csv"

// Cloud Path
var CLOUD_EXPORT_PATH string = "../mongo/communication/cloud/export/"
var CLOUD_RECEIVE_PATH string = "../mongo/communication/cloud/receive/"
var CLOUD_EXPORT_FILE string = CLOUD_EXPORT_PATH + "exported.csv"

// Replication Path
var REPLICATION_SEND_PATH string = "../mongo/communication/repl/send/"
var REPLICATION_RECEIVE_PATH string = "../mongo/communication/repl/receive/"
var REPLICATION_SEND_FILE string = REPLICATION_SEND_PATH + "exported.csv"
var REPLICATION_RECEIVE_FILE string = REPLICATION_RECEIVE_PATH + "received.csv"
var REPLICATION_EXPORT_FILE string = REPLICATION_RECEIVE_PATH + "exported.csv"

// Reconciliation Path
var RECONCILIATION_SEND_PATH string = "../mongo/communication/recon/send/"
var RECONCILIATION_RECEIVE_PATH string = "../mongo/communication/recon/receive/"
var RECONCILIATION_SEND_FILE string = RECONCILIATION_SEND_PATH + "exported.csv"
var RECONCILIATION_RECEIVE_FILE string = RECONCILIATION_RECEIVE_PATH + "received.csv"
var RECONCILIATION_EXPORT_FILE string = RECONCILIATION_RECEIVE_PATH + "exported.csv"

// Migration Path
var MIGRATION_SEND_PATH string = "../mongo/communication/migr/send/"
var MIGRATION_RECEIVE_PATH string = "../mongo/communication/migr/receive/"
var MIGRATION_SEND_FILE string = MIGRATION_SEND_PATH + "exported.csv"
var MIGRATION_RECEIVE_FILE string = MIGRATION_RECEIVE_PATH + "received.csv"
var MIGRATION_EXPORT_FILE string = MIGRATION_RECEIVE_PATH + "exported.csv"
