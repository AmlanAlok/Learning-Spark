

How to run on local machine?

mvn install

spark-submit --class Graph target/cse6331-project5-0.1.jar small-graph.csv

or

spark-submit --class Graph target/*.jar small-graph.csv



[amlanalok@login02 ~]$ module load sdsc
[amlanalok@login02 ~]$ expanse-client user -r expanse

Resource  expanse

╭───┬───────────┬───────┬─────────┬──────────────┬──────┬───────────┬─────────────────╮
│   │ NAME      │ STATE │ PROJECT │ TG PROJECT   │ USED │ AVAILABLE │ USED BY PROJECT │
├───┼───────────┼───────┼─────────┼──────────────┼──────┼───────────┼─────────────────┤
│ 1 │ amlanalok │ allow │ uot143  │ TG-CIE160028 │   82 │     30000 │           24390 │
╰───┴───────────┴───────┴─────────┴──────────────┴──────┴───────────┴─────────────────╯
[amlanalok@login02 ~]$ 

[amlanalok@login02 project5]$ sbatch graph.local.run
Submitted batch job 11193144

[amlanalok@login02 project5]$ sbatch graph.distr.run
Submitted batch job 11194200

[amlanalok@login01 ~]$ module load sdsc
[amlanalok@login01 ~]$ expanse-client user -r expanse

Resource  expanse

╭───┬───────────┬───────┬─────────┬──────────────┬──────┬───────────┬─────────────────╮
│   │ NAME      │ STATE │ PROJECT │ TG PROJECT   │ USED │ AVAILABLE │ USED BY PROJECT │
├───┼───────────┼───────┼─────────┼──────────────┼──────┼───────────┼─────────────────┤
│ 1 │ amlanalok │ allow │ uot143  │ TG-CIE160028 │   87 │     30000 │           24395 │
╰───┴───────────┴───────┴─────────┴──────────────┴──────┴───────────┴─────────────────╯
[amlanalok@login01 ~]$

[amlanalok@login02 p5]$ sbatch graph.local.run
Submitted batch job 11201381
[amlanalok@login02 p5]$ sbatch graph.distr.run
Submitted batch job 11201389

[amlanalok@login02 p5-2]$ sbatch graph.distr.run
Submitted batch job 11206133

[amlanalok@login02 p5]$ sbatch graph.distr.run
Submitted batch job 11206206

[amlanalok@login01 ~]$ module load sdsc
[amlanalok@login01 ~]$ expanse-client user -r expanse

[amlanalok@login01 project5]$ sbatch graph.distr.run
Submitted batch job 11213602

Resource  expanse

╭───┬───────────┬───────┬─────────┬──────────────┬──────┬───────────┬─────────────────╮
│   │ NAME      │ STATE │ PROJECT │ TG PROJECT   │ USED │ AVAILABLE │ USED BY PROJECT │
├───┼───────────┼───────┼─────────┼──────────────┼──────┼───────────┼─────────────────┤
│ 1 │ amlanalok │ allow │ uot143  │ TG-CIE160028 │  121 │     30000 │           24479 │
╰───┴───────────┴───────┴─────────┴──────────────┴──────┴───────────┴─────────────────╯

[amlanalok@login01 ~]$ expanse-client user -r expanse

Resource  expanse

╭───┬───────────┬───────┬─────────┬──────────────┬──────┬───────────┬─────────────────╮
│   │ NAME      │ STATE │ PROJECT │ TG PROJECT   │ USED │ AVAILABLE │ USED BY PROJECT │
├───┼───────────┼───────┼─────────┼──────────────┼──────┼───────────┼─────────────────┤
│ 1 │ amlanalok │ allow │ uot143  │ TG-CIE160028 │  135 │     30000 │           24495 │
╰───┴───────────┴───────┴─────────┴──────────────┴──────┴───────────┴─────────────────╯


Issue

I did not put the check for start_id in the large-twitter.csv dataset in my code.
So the distance 0 was never getting assigned to any K, V pair.
This created an infinite loop and caused the java out of memory error.
That is why only 5 SUs got consumed for every distributed run.
Finally, when I fixed the code, it consumed 18 SUs.

I need to be careful next time and do not forget minor details.


2nd Mistake
Used the to keyword instead of until.
So, my loop was running from 0 to 5 (6 times)
It should be from 0 to 4 (5 times)