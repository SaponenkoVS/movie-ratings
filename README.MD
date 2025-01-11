# Mapreduce with MRJob

1. From Azure portal copy cluster ssh address.
    Grupy zasobów  -> "name of the group" (ex bigdata) -> enter to the "HDInsight cluster" -> Properties -> Copy Secure Shell (SSH).

2. From local shell copy example_2 to cluster:
```console
	$ ssh sshuser@[ssh_address] "mkdir -p /home/sshuser/example_2"
	$ scp -r  [git repo path]* sshuser@[ssh_address]:/home/sshuser/example_2
```
**where:**<br/>
* sshuser - is default name created by the Azure<br/>
* [ssh_address] - address from the first step.
* [git repo path] - local git repository -> C:/Users/vdi-student/hadoop/example_2/

3. Login to the cluster:
```console
    $ ssh sshuser@[ssh_address]
```
**where:**<br/>
* ssh_address - address from the first step.

4. Install mrjob:
```console
    $ sudo pip install mrjob
```

5. Run script only local:
```console
    $ python /home/sshuser/example_2/ratingcount.py /home/sshuser/example_2/data.txt
```

## Additional steps:
6. From cluster shell copy data.txt to hdfs:
```console
    $ hdfs dfs -copyFromLocal /home/sshuser/example_2/data.txt /user/data.txt
```
7. Run script on cluster:
```console
    $ python ./example_2/ratingcount.py wasb:///user/data.txt -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar --hadoop-tmp-dir wasb:////tmp
```

