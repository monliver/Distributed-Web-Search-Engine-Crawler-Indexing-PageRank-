 # set persissions
 chmod 400 megasearch.pem
 # permission grants to run this script
 chmod +x ci_cd/build.sh     
#  kill all ports: 
 lsof -ti:8000,8001,8002,8003,9000,9001,9002,9003, 444 2>/dev/null | xargs kill -9 2>/dev/null || true
#  peek the ec2 files: 
 ls database/kvs_workers/worker1
# check log
tail -n 5 logs/kvs-worker{1,2,3}.log
#  peek ec2 file content: 
 cat database/kvs_workers/worker1/pt-crawl/_fe/feskhmaovewgacvgjifqoiqonkqsdocigcpksgpi
#  check the total number in pt-crawl file: 
 find database/kvs_workers/worker*/pt-crawl -type f | wc -l
#  check the number in pt-crawl file: 
 find database/kvs_workers/worker1/pt-crawl -type f | wc -l; find database/kvs_workers/worker2/pt-crawl -type f | wc -l; find database/kvs_workers/worker3/pt-crawl -type f | wc -l;
#  start ec2: 
 sudo ssh ec2-user@3.13.7.180 -i megasearch.pem
# delet all lagecy files
rm -rf __MACOSX database/ ci_cd/ job-*.jar logs/ lib/
#  dns setup website: 
 http://dns.cis5550.net/
 #  Access Application in production
https://megasearch.cis5550.net/
# check worker memory allocation
ps aux | grep -E "[j]ava .*kvs.Worker|[j]ava .*flame.Worker"
# free memory
sudo sync;sudo sh-c"echo 1>/proc/sys/vm/drop_caches'

Environment Variables (set once in terminal):
  export EC2_IP="3.13.7.180"           # Your EC2 instance IP
  export EC2_KEY="megasearch.pem"          # Path to your SSH key file
  export WORKER_COUNT=


Usage:
  ci_cd/build.sh build                            # compile sources and create JAR
  ci_cd/build.sh start [--wipe-db]                # start coordinators + $WORKER_COUNT workers
  ci_cd/build.sh stop                             # stop all services
  ci_cd/build.sh status                           # show status
  ci_cd/build.sh restart                          # stop then start
  ci_cd/build.sh crawler [--fresh] [--resume] [--target N] [URL]  # submit crawler job
  ci_cd/build.sh indexer                          # submit indexer job
  ci_cd/build.sh pagerank                         # submit pagerank job
  ci_cd/build.sh tfidf                            # submit tf-idf job
  ci_cd/build.sh pipeline                         # run crawler → indexer → pagerank → tf-idf
  sudo ci_cd/build.sh webapp 8080                  # start webapp (default 8080 local, 444 EC2)
  ci_cd/build.sh deploy                           # copy JAR and build script (uses $EC2_IP $EC2_KEY)
  scp -i "$EC2_KEY" database.zip ec2-user@"$EC2_IP":~/. # send extra files
  ci_cd/build.sh remote-start [start options]     # copy & start (uses $EC2_IP $EC2_KEY)


##  Alternative way to run data processing jobs on EC2
# 1. Crawler
java -cp lib/megasearch.jar flame.FlameSubmit localhost:9000 lib/megasearch.jar jobs.Crawler
# 2. Indexer
java -cp lib/megasearch.jar flame.FlameSubmit localhost:9000 lib/megasearch.jar jobs.Indexer
# 3. PageRank
java -cp lib/megasearch.jar flame.FlameSubmit localhost:9000 lib/megasearch.jar jobs.PageRank
# 4. TF-IDF
java -cp lib/megasearch.jar flame.FlameSubmit localhost:9000 lib/megasearch.jar jobs.TfIdf
# 5. run web server
sudo java -cp lib/megasearch.jar frontend.app.WebApp 444

---------- EC2 Setup -------------
Set environment variables (do this once per terminal session):
  export EC2_IP="3.13.7.180"
  export EC2_KEY="megasearch.pem"
  chmod 400 "$EC2_KEY"

Deploy and start:
  ci_cd/build.sh remote-start

Or manually:
  ssh -i "$EC2_KEY" ec2-user@"$EC2_IP" 'mkdir -p lib ci_cd'
  scp -i "$EC2_KEY" lib/megasearch.jar ec2-user@"$EC2_IP":lib/
  scp -i "$EC2_KEY" ci_cd/build.sh ec2-user@"$EC2_IP":ci_cd/
  ssh -i "$EC2_KEY" ec2-user@"$EC2_IP" 'export WORKER_COUNT=1 && chmod +x ci_cd/build.sh && ci_cd/build.sh start'

Submit jobs on EC2:
  ssh -i "$EC2_KEY" ec2-user@"$EC2_IP" 'ci_cd/build.sh crawler'
  ssh -i "$EC2_KEY" ec2-user@"$EC2_IP" 'ci_cd/build.sh indexer'
  ssh -i "$EC2_KEY" ec2-user@"$EC2_IP" 'ci_cd/build.sh pagerank'
  ssh -i "$EC2_KEY" ec2-user@"$EC2_IP" 'ci_cd/build.sh tfidf'
  ssh -i "$EC2_KEY" ec2-user@"$EC2_IP" 'ci_cd/build.sh pipeline'



# how to run crawler without stopping when closing laptop:
ssh -i megasearch.pem ec2-user@3.13.7.180
screen -S crawler
ci_cd/build.sh crawler
# Press Ctrl+A then D to detach
# Close laptop safely
# Later: ssh back in and run: screen -r crawler

# how to kill 8080 up
sudo lsof -ti:8080
sudo kill -9 <pid>


-------- MegaSearch Deployment Guide -------------
## AWS Configuration
Elastic IP: ***

## Initial SSH Connection
sudo ssh ec2-user@3.13.7.180 -i megasearch.pem
# yes


## Fix key permissions if needed:
chmod 400 MegaSearch_keypair.pem
ssh ec2-user@3.13.7.180 -i MegaSearch_keypair.pem


## EC2 Instance Setup
# Configure EC2 instance(Install Java and Certbot )
sudo dnf install -y java augeas-libs
sudo python3 -m venv /opt/certbot/
sudo /opt/certbot/bin/pip install --upgrade pip
sudo /opt/certbot/bin/pip install certbot


## DNS Configuration
DNS account 
Username: megasearch
Password: *****

Please use the same web site (https://urldefense.com/v3/__http://dns.cis5550.net/__;!!IBzWLUs!RUEdzL8kCJkGeqkiI80NI7bxDbvgTV-0BkrRsGjvvY5Pl4hQz4hajYqFL_2wPmbPcfNL_6hM-M8e-HFq9Qjz2A$) 
and the same process as in HW3 to set up your subdomain and to get your TLS certificate.


## TLS Certificate Setup
sudo /opt/certbot/bin/certbot certonly --standalone -d megasearch.cis5550.net
# email address:no
# When prompted: choose option 1 to keep existing certificate
# Certificate is saved at: /etc/letsencrypt/live/megasearch.cis5550.net/fullchain.pem
# Key is saved at:         /etc/letsencrypt/live/megasearch.cis5550.net/privkey.pem
# This certificate expires on 2026-02-25.

## Convert certificate to PKCS12 format:
sudo openssl pkcs12 -export -in /etc/letsencrypt/live/megasearch.cis5550.net/fullchain.pem \
-inkey /etc/letsencrypt/live/megasearch.cis5550.net/privkey.pem \
-out /home/ec2-user/keystore.p12 -name megasearch.cis5550.net \
-CAfile /etc/letsencrypt/live/megasearch.cis5550.net/fullchain.pem \
-caname "Let’s Encrypt Authority X3" -password pass:secret

## Set permissions:
sudo chmod 644 keystore.p12
keytool -importkeystore -deststorepass secret -destkeypass secret \
-deststoretype pkcs12 -srckeystore /home/ec2-user/keystore.p12 \
-srcstoretype PKCS12 -srcstorepass secret -alias megasearch.cis5550.net \
-destkeystore /home/ec2-user/keystore.jks
# yes
