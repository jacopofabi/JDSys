# INSTALLAZIONE
Per l'installazione dell'applicazione è necessario l'utilizzo dei servizi web offerti da Amazon (AWS): 

1. Creare un'istanza EC2 che assume il compito di service registry
2. Installare su di essa Git per il download del codice sorgente e Go per poter eseguire l'applicazione 
3. Inserire in /home/ec2-user/.aws il file "*credentials*" dell'account di AWS utilizzato
4. Copiare il servizio "*registry.service*" in /etc/systemd/system ed avviarlo
<br>

Per la gestione e l'evoluzione del sistema è necessario:
1. Creare un LB per l'instradamento delle richieste verso i nodi
2. Creare un'AMI da un'istanza EC2 
3. Installare su di essa Git per il download del codice sorgente, Go per poter eseguire l'applicazione e MongoDB per poter memorizzare e gestire i dati del sistema di storage
4. Inserire in /home/ec2-user/.aws il file "*credentials*" dell'account di AWS utilizzato
5. Copiare  il servizio "*node.service*" in /etc/systemd/system e abilitarlo così che venga avviato all'avvio del sistema
6. Creare la configurazione di avvio per l'Autoscaling tramite l'AMI realizzata al punto 2
7. Creare l'Autoscaling e i corrispondenti allarmi per la gestione e realizzazione di *scaleIn/scaleOut*
<br><br>

# CONFIGURAZIONE
Per configurare l'applicazione è sufficiente modificare il file "Configuration.go", in cui sono definiti tutti i parametri che vengono utilizzati:
1. Aggiornare **ELB_ARN** con quello creato
2. Aggiornare **AUTOSCALING_NAME** con quello creato
3. Aggiornare **LB_DNS_NAME** con quello creato
4. Aggiornare **REGISTRY_IP** con quello dell'istanza utilizzata
<br>

NOTA: Oltre agli altri parametri di configurazione, è possibile modificare anche le porte utilizzate dall'applicazione, tenere a mente che, per la porta utilizzata dal LB, non basta modificarla sul codice sorgente ma bisogna aggiornarla anche nelle impostazioni dalla console AWS, modificando la porta utilizzata per gli "*healthy check*" dei nodi. 
<br><br>

# ESECUZIONE
Una volta configurato il back-end del sistema come spiegato in maniera dettagliata nella sezione "**INSTALLAZIONE**", è sufficiente: 
1. Scaricare in locale il codice sorgente da Github 
2. Eseguire il file "*client.go*" per poter interagire con i nodi e quindi con il sistema di storage distribuito
<br>

NOTA: ovviamente, è necessario avere installato Go sulla macchina, altrimenti non sarà possibile eseguire l'applicazione lato client.
<br><br>