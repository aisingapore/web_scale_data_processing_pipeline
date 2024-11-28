# setup on fresh VM

1. Create venv, install requirements, then Install Java
```bash
sudo apt update
sudo apt install openjdk-11-jdk
java -version

echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc

source ~/.bashrc
echo $JAVA_HOME
```

# Transferring Files
1. From source machine, copy destination machine's private key to source directory.
   ```eval "$(ssh-agent -s)"```
2. Start SSH agent if necessary, then add the private key:
   ```ssh-add ~/.ssh/id_ed25519```
3. List added identities:
   ```ssh-add -L```
4. SSH into destination machine (from source machine) with agent mode using ```ssh -A USERNAME-2@SERVER-IP-2```
   - Manually type YES when prompted.
5. Should be able to ssh directly into remote machine using normal ssh user2@server2-IP


* finally, launch copying from source machine where 
```bash
rsync -av --info=progress2 /data/users/brandon/ob1-projects/data_processing/indo_journals_subsets/subset_6 USERNAME-2@SERVER-IP-2:REMOTE-PATH
```

# Usage
1. rsync subset into indo_journals_subsets and ensure path is correct 
   * nested path common error: {base_url}/indo_journals_subsets/subset_1/subset_1
2. copy over corresponding csv file containing list of pdfs that have been quality checked (filtering is the term used for now though no explicitly filtering done yet), contains is_relevant column
   * i.e. subset_1_filtered.csv
3. use process.ipynb to change the base_url for pdf_path and test 1 example so pipeline can load from local
4. check base_urls and filepaths are correct in mathpix_spark_pipeline.py before running. 
   * errors should only occur for incorrect filepaths without a valid base_url

Estimated Time Stats to process rows where "is_relevant=True":
A100 40GB (Max Workers = 5): 
   * Subset1 hours); Average time per row: 1.73 seconds
   * Subset 4: 7898 rows; Total duration: 14901.58 seconds (4.1 hours); Average time per row: 1.89 seconds
A6000 48GB (Max Workers = 5): 
   * Subset 2: 6939 rows; Total duration: 12999.14 seconds (3.6 hours); Average time per row: 1.87 seconds
H100 80GB PCIE (Max Workers = 8): 
   * Subset 3: 7149 rows; Total duration: 7185.48 seconds (2 hours); Average time per row: 1.01 seconds