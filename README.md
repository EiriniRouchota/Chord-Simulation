# Chord-Simulation
:pushpin: Μία [ρεαλιστική](#Ρεαλιστική-Προσέγγιση) υλοποίηση βασικών λειτουργιών του πρωτοκόλλου Chord όπως αναλύονται στο [[1]](#Βιβλιογραφία). 

# Υλοποιημένες Λειτουργίες

- **insert a new (key/value) pair** in the Chord ring</br>
 - **delete a (key/value) pair** from the Chord ring</br>
 - **update value** in an existing (key/value) pair</br>
 - perform an **exact match in the network (Lookup)**</br>
 - display **current Chord ring configuration**</br>
 - **add a new Node (Join)** in the Chord ring</br>
 - **delete an existing Node (Leave)** from the Chord ring</br>
-  run a **complete benchmark** on the current Chord ring</br>

# How to run

**usage:** Chord.py [-h] [-n N] [-fn FN] [-d D] [-fr FR] [-fs FS]

**Implementing Chord**

optional arguments:
<pre>
  -h, --help  show this help message and exit
  -n 	N        N is number of initial numbers of nodes withing Chord ring
  -fn 	FN      FN is the input csv file name to read data from
  -d 	D        D is the number of data records to be loaded from the input csv file
  -fr 	FR      FR is the number of the stored successor failure recovery. If
                not set no failure recovery action will be used
  -fs 	FS      FS is the name of the file in which statistics will be written
</pre>

## Screenshots 

![image](https://user-images.githubusercontent.com/68953073/156058306-3662152d-975b-49fb-97cd-a5ec3cd5c0c3.png)

![image](https://user-images.githubusercontent.com/68953073/156058163-a632be0b-590d-45d1-8fa7-e1339c8fc380.png)

### Ρεαλιστική Προσέγγιση 
1.	Προφανής επέκτασή της σε πραγματικό δίκτυο με υπολογιστές που ο καθένας έχει τη δική του διεύθυνση (ip, port) επικοινωνίας.
2.	Η υλοποίηση με threads απεικονίζει κατάλληλα την παράλληλη εκτέλεση διαφόρων λειτουργιών τόσο μέσα σε κάθε κόμβο όσο και σε όλους τους κόμβους ταυτόχρονα. Ο μόνος περιορισμός σε αυτό προέρχεται από το λειτουργικό σύστημα και τον υπολογιστή στον οποίο εκτελείται η προσομοίωση.

#### Τo be done
- Ολοκλήρωση failure recovery
- Επέκταση σε δίκτυο με πραγματικούς υπολογιστές 


### Βιβλιογραφία

[1] ION STOICA AND ROBERT MORRIS AND DAVID LIBEN-NOWELL AND DAVID R. KARGER AND M. FRANS KAASHOEK AND FRANK DABEK AND HARI BALA-KRISHNAN, Chord: A scalable peer-to-peer lookup protocol for Internet applica-tions, IEEE/ACM Transactions on Networking, Vol 11/2003, ISSN: 10636692, Is-sue1, DOI: 10.1109/TNET.2002.808407,

[2] STOICA, I., MORRIS, R., KARGER, D., KAASHOEK, M. F., AND BALAKRISHNAN, H. Chord: A scalable peer-to-peer lookup service for internet applications. Tech. Rep. TR-819, MIT LCS, March 2001. http://www.pdos.lcs.mit.edu/chord/papers/.


###### Documentation report available 
Further information :question: Don't hesitate to contact me !
