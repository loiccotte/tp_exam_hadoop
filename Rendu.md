# TP Hadoop MapReduce - tags.csv (MovieLens 25M)

## Le fichier

On bosse sur `ml-25m/tags.csv`, environ 38 Mo. C'est un CSV classique :

```
userId,movieId,tag,timestamp
3,260,classic,1439472355
3,260,sci-fi,1439472256
4,1732,dark comedy,1573943598
```

Quatre colonnes separees par des virgules, avec un header en premiere ligne. Le header ne fait pas planter le `split` (il a bien 4 champs), du coup il faut le filtrer a la main dans les mappers.

Un truc qu'on a remarque en testant : certains tags contiennent des virgules (genre "dark, brooding"). On utilise `split(',', 3)` qui coupe en 4 morceaux max, donc le tag recupere tout ce qui reste apres movieId. Ca marche dans la majorite des cas. On aurait pu utiliser `csv.reader` pour etre plus propre, mais on a prefere rester sur l'approche du cours.

## Environnement

VM Hortonworks HDP Sandbox sur RedHat. On se connecte en SSH : `ssh maria_dev@localhost -p 2222`. On a verifie que Hadoop tournait bien avec `jps` avant de commencer.

## Preparation

Premier reflexe : se faire un petit echantillon avant de lancer sur les 38 Mo. On a fait le test sans au debut et on attendait 3-4 minutes a chaque run pour debugger une typo, c'etait penible.

```bash
hdfs dfs -cat ml-25m/tags.csv | head -100 > tags_sample.csv
hdfs dfs -put tags_sample.csv tags_sample.csv
```

Ca nous donne 99 lignes de donnees (+ le header) pour iterer vite.

---

## Partie 1 - Config Hadoop par defaut

### Q1 : Combien de tags par film ?

L'idee est simple : le mapper sort `(movieId, 1)` pour chaque ligne, le reducer additionne. On a ajoute un `if` pour virer le header (au debut on l'avait pas et le header comptait comme un film, on s'en est rendu compte en voyant "movieId" dans les resultats).

```python
# tags_par_film.py
from mrjob.job import MRJob

class TagsParFilm(MRJob):
    def mapper(self, _, line):
        try:
            (userID, movieID, tag, timestamp) = line.split(',', 3)
            if userID == 'userId':
                return
            yield movieID, 1
        except Exception:
            pass

    def reducer(self, movieID, counts):
        yield movieID, sum(counts)

if __name__ == '__main__':
    TagsParFilm.run()
```

Test sur l'echantillon d'abord :

```bash
python tags_par_film.py -r hadoop \
    --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    hdfs:///user/maria_dev/tags_sample.csv \
    -o hdfs:///user/maria_dev/output_tags_film_test
```

Resultat sur l'echantillon (99 lignes, 27 films) :

```
"102445"        3
"104841"        1
"109487"        9
"1127"          7
"115569"        1
"115713"        3
"1210"          1
"148426"        1
"1619"          6
"164909"        2
"168250"        2
"1719"          10
"1732"          2
"194728"        3
"215"           14
"2160"          1
"260"           2
"3481"          1
"434"           1
"44665"         1
"590"           9
"6537"          7
"7099"          7
"72998"         3
"7569"          1
"79132"         1
```

Ca a l'air correct. Au passage, dans notre premiere version du script on n'avait pas le filtre `if userID == 'userId'`, et on se retrouvait avec une ligne `"movieId" 1` dans les resultats. On a mis un moment a comprendre d'ou ca venait avant de realiser que le header passait le `split` sans erreur. D'ou le filtre explicite.

On lance sur le fichier complet :

```bash
python tags_par_film.py -r hadoop \
    --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    hdfs:///user/maria_dev/ml-25m/tags.csv \
    -o hdfs:///user/maria_dev/output_tags_film
```

Temps d'execution : environ **1min04**. 1 093 361 lignes traitees, 45 252 films distincts. Extrait :

```
"1"         697
"10"        137
"100"       18
"1000"      10
"100001"    1
"100003"    3
"100008"    9
"100017"    9
"100032"    2
"100034"    19
```

Ce qui saute aux yeux : le film 1 (Toy Story) a 697 tags, alors que la plupart des films en ont moins de 10. Les gros classiques concentrent une part enorme des tags. En moyenne c'est ~24 tags par film (1 093 360 / 45 252), mais la mediane est surement bien plus basse vu la distribution.

Resultats complets : [Lien GitHub](TODO)

---

### Q2 : Combien de tags par utilisateur ?

Meme logique que Q1 sauf qu'on yield `userId` au lieu de `movieId`. Pas grand chose a changer dans le code :

```python
# tags_par_user.py
from mrjob.job import MRJob

class TagsParUser(MRJob):
    def mapper(self, _, line):
        try:
            (userID, movieID, tag, timestamp) = line.split(',', 3)
            if userID == 'userId':
                return
            yield userID, 1
        except Exception:
            pass

    def reducer(self, userID, counts):
        yield userID, sum(counts)

if __name__ == '__main__':
    TagsParUser.run()
```

Test sur l'echantillon d'abord (9 utilisateurs distincts sur 99 lignes) :

```bash
python tags_par_user.py -r hadoop \
    --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    hdfs:///user/maria_dev/tags_sample.csv \
    -o hdfs:///user/maria_dev/output_tags_user_test
```

```
"19"    8
"20"    1
"3"     2
"4"     13
"43"    1
"68"    1
"84"    3
"87"    31
"91"    39
```

L'utilisateur 91 domine l'echantillon avec 39 tags sur 99. On lance sur le complet :

```bash
python tags_par_user.py -r hadoop \
    --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    hdfs:///user/maria_dev/ml-25m/tags.csv \
    -o hdfs:///user/maria_dev/output_tags_user
```

Temps d'execution : environ **1min08**. 14 593 utilisateurs distincts. Extrait :

```
"100001"        9
"100016"        50
"100028"        4
"100029"        1
"100033"        1
"100046"        133
"100051"        19
"100058"        5
"100065"        2
"100068"        19
```

Meme constat que pour les films : c'est tres desequilibre. La moyenne est de ~75 tags par utilisateur (1 093 360 / 14 593), mais la plupart en ont pose moins de 10. Quelques utilisateurs tres actifs (genre 684 tags pour l'un d'entre eux) tirent la moyenne vers le haut. C'est une distribution typique "longue traine" qu'on retrouve souvent dans les donnees de tagging collaboratif.

Resultats complets : [Lien GitHub](TODO)

---

## Partie 2 - Config avec blocs de 64 Mo

### Q3 : Nombre de blocs dans chaque config

HDFS decoupe les fichiers en blocs. La taille par defaut est 128 Mo (depuis Hadoop 2.x, avant c'etait 64 Mo).

Notre `tags.csv` fait 38 Mo. Comme 38 < 64 < 128, il tient dans un seul bloc meme en config 64 Mo. On s'attendait a voir une difference, mais non. Pour que ca change il faudrait un fichier plus gros que 64 Mo (genre `ratings.csv` qui fait 678 Mo, lui il serait decoupe en 11 blocs en 64 Mo vs 6 en 128 Mo).

On verifie quand meme. D'abord on recupere le fichier en local puis on le re-uploade avec un blocksize de 64 Mo :

```bash
hdfs dfs -get ml-25m/tags.csv tags.csv

# Config par defaut (128 Mo)
hdfs fsck ml-25m/tags.csv -files -blocks

# Upload force en 64 Mo
hdfs dfs -D dfs.blocksize=67108864 -put tags.csv tags_64M.csv

# Verification
hdfs fsck tags_64M.csv -files -blocks
```

Config par defaut (128 Mo) :

```
/user/maria_dev/ml-25m/tags.csv 38810332 bytes, 1 block(s):  OK
0. blk_1073743068_2248 len=38810332 repl=1
Total blocks (validated):      1 (avg. block size 38810332 B)
```

Config 64 Mo :

```
/user/maria_dev/tags_64M.csv 38810332 bytes, 1 block(s):  OK
0. blk_1073743387_2571 len=38810332 repl=1
Total blocks (validated):      1 (avg. block size 38810332 B)
```

**1 bloc dans les deux cas.** 38 810 332 octets (~37 Mo), en dessous des deux seuils. Le nombre de mappers lances sera aussi identique (1 mapper = 1 bloc), donc aucun gain de parallelisme dans notre cas.

Pour illustrer ce que ca donnerait sur un fichier plus gros, on a fait le test avec `ratings.csv` (678 Mo) :

```bash
hdfs fsck ml-25m/ratings.csv -files -blocks
hdfs dfs -D dfs.blocksize=67108864 -put ratings.csv ratings_64M.csv
hdfs fsck ratings_64M.csv -files -blocks
```

Config par defaut (128 Mo) : **6 blocs**

```
/user/maria_dev/ml-25m/ratings.csv 678260987 bytes, 6 block(s):  OK
0. blk_1073743062 len=134217728   (128 Mo)
1. blk_1073743063 len=134217728
2. blk_1073743064 len=134217728
3. blk_1073743065 len=134217728
4. blk_1073743066 len=134217728
5. blk_1073743067 len=7172347     (6.8 Mo, le reste)
```

Config 64 Mo : **11 blocs**

```
/user/maria_dev/ratings_64M.csv 678260987 bytes, 11 block(s):  OK
0.  blk_1073743493 len=67108864   (64 Mo)
1.  blk_1073743494 len=67108864
2.  blk_1073743495 len=67108864
...
9.  blk_1073743502 len=67108864
10. blk_1073743503 len=7172347    (6.8 Mo, le reste)
```

La c'est parlant : avec des blocs de 64 Mo, Hadoop pourrait lancer 11 mappers en parallele au lieu de 6. Sur un cluster avec assez de noeuds, ca reduirait le temps de traitement. Le dernier bloc fait 6.8 Mo dans les deux cas (c'est le reste de la division).

---

### Q4 : Combien de fois chaque tag a ete utilise ?

On compte les occurrences de chaque tag. On normalise en minuscules avec `.lower()` parce que sinon "sci-fi", "Sci-Fi" et "Sci-fi" comptent comme 3 tags differents. C'est un choix : on perd la casse originale, mais on gagne en coherence. On fait aussi un `.strip()` pour virer les espaces en trop.

```python
# frequence_tags.py
from mrjob.job import MRJob

class FrequenceTags(MRJob):
    def mapper(self, _, line):
        try:
            (userID, movieID, tag, timestamp) = line.split(',', 3)
            if userID == 'userId':
                return
            yield tag.strip().lower(), 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    FrequenceTags.run()
```

On lance sur le fichier en 64 Mo (partie 2 de la consigne) :

```bash
python frequence_tags.py -r hadoop \
    --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    hdfs:///user/maria_dev/tags_64M.csv \
    -o hdfs:///user/maria_dev/output_frequence_tags
```

Temps d'execution : environ **1min06**. 65 372 tags distincts sur 1 093 360 lignes de donnees (le header a bien ete filtre, on a verifie). Extrait :

```
"#1 prediction"                                  3
"#adventure"                                     1
"#boring"                                        1
"#danish"                                        2
"#fantasy"                                       2
"#thriller #suspense #disjointed #boring"         1
"#wtf"                                           1
```

C'est assez parlant : la grande majorite des tags n'apparaissent qu'une seule fois (des hapax). On voit aussi des hashtags, des phrases entieres utilisees comme tags, des fautes de frappe... Le dataset est tres "bruité" parce que les tags sont du texte libre saisi par les utilisateurs, sans aucune contrainte. Ca explique pourquoi on a 65 000 tags differents pour seulement 1M de lignes : le ratio tags uniques / total est de ~6%, ce qui est tres eleve.

Resultats complets : [Lien GitHub](TODO)

---

### Q5 : Pour chaque film, combien de tags par utilisateur ?

La il faut grouper par couple (movieId, userId). Le probleme c'est que MRJob veut une cle string. On a essaye avec un tuple au debut, mais MRJob le serialise en liste JSON de toute facon, donc autant le faire nous-memes avec `json.dumps`. Le tri est lexicographique sur la string, ca groupe bien par film puis par utilisateur.

```python
# tags_user_film.py
from mrjob.job import MRJob
import json

class TagsUserFilm(MRJob):
    def mapper(self, _, line):
        try:
            (userID, movieID, tag, timestamp) = line.split(',', 3)
            if userID == 'userId':
                return
            yield json.dumps([movieID, userID]), 1
        except Exception:
            pass

    def reducer(self, key, counts):
        yield key, sum(counts)

if __name__ == '__main__':
    TagsUserFilm.run()
```

```bash
python tags_user_film.py -r hadoop \
    --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    hdfs:///user/maria_dev/tags_64M.csv \
    -o hdfs:///user/maria_dev/output_tags_user_film
```

Temps d'execution : environ **1min17** (le plus long, a cause de la cle composite JSON plus lourde a trier). 305 356 couples (film, utilisateur) distincts. Extrait :

```
["100003", "6550"]        2
["100008", "21096"]       2
["100008", "6550"]        6
["100017", "6550"]        7
["100034", "14116"]       9
["100034", "129101"]      3
["100044", "31047"]       4
["100044", "58039"]       4
```

L'utilisateur 6550 revient souvent, c'est probablement un des gros tagueurs du dataset. La plupart des couples ont 1 ou 2 tags. 9 tags d'un meme utilisateur sur un meme film (utilisateur 14116 sur le film 100034), ca fait beaucoup, cette personne avait visiblement des choses a dire.

En croisant avec Q2 : les 14 593 utilisateurs se repartissent sur 305 356 couples film/user, soit en moyenne ~21 films tagges par utilisateur. Mais la encore, la distribution est probablement tres asymetrique.

Resultats complets : [Lien GitHub](TODO)

---

## Pour aller plus loin

Quelques observations en recoupant les resultats :

- **Distribution desequilibree partout** : que ce soit les tags par film, par utilisateur, ou la frequence des tags, on a toujours une minorite qui concentre l'essentiel de l'activite. C'est coherent avec la loi de Zipf / le principe de Pareto qu'on retrouve dans beaucoup de donnees generees par des utilisateurs.
- **Limites du MapReduce ici** : pour un fichier de 38 Mo qui tient en 1 bloc, Hadoop n'apporte pas grand chose par rapport a un simple `awk` ou `pandas`. L'interet serait sur un fichier beaucoup plus gros ou le parallelisme sur plusieurs blocs / noeuds ferait la difference.
- **Qualite des tags** : le fait d'avoir 65 000 tags uniques pour 1M de lignes montre que le tagging libre sans vocabulaire controle genere enormement de bruit. Un systeme de suggestions ou d'autocompletion reduirait ca.
