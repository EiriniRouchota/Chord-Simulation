# Chord-Simulation
:pushpin: Μία [ρεαλιστική](#Ρεαλιστική-Προσέγγιση) υλοποίηση βασικών λειτουργιών του πρωτοκόλου Chord όπως αναλύονται στο [[1]](#Βιβλιογραφία). 

# Υλοποιημένες Λειτουργίες

Type [1] to insert a new (key/value) pair in the Chord ring

Type [2] to delete a (key/value) pair from the Chord ring

Type [3] to update value in an existing (key/value) pair

Type [4] to perform an exact match in the network (Lookup)
Type [5] to display current Chord ring configuration
Type [6] to add a new Node (Join) in the Chord ring
Type [7] to delete an existing Node (Leave) from the Chord ring
Type [8] to run a complete benchmark on the current Chord ring
Type [0] to exit

### Ρεαλιστική Προσέγγιση 
1.	Προφανής επέκτασή της σε πραγματικό δίκτυο με υπολογιστές που ο καθένας έχει τη δική του διεύθυνση (ip, port) επικοινωνίας.
2.	Η υλοποίηση με threads απεικονίζει κατάλληλα την παράλληλη εκτέλεση διαφόρων λειτουργιών τόσο μέσα σε κάθε κόμβο όσο και σε όλους τους κόμβους ταυτόχρονα. Ο μόνος περιορισμός σε αυτό προέρχεται από το λειτουργικό σύστημα και τον υπολογιστή στον οποίο εκτελείται η προσομοίωση.

#### Τo be done
- Ολοκλήρωση failure recovery
- Επέκταση σε δίκτυο με πραγματικούς υπολογιστές 


### Βιβλιογραφία

[1] ION STOICA AND ROBERT MORRIS AND DAVID LIBEN-NOWELL AND DAVID R. KARGER AND M. FRANS KAASHOEK AND FRANK DABEK AND HARI BALA-KRISHNAN, Chord: A scalable peer-to-peer lookup protocol for Internet applica-tions, IEEE/ACM Transactions on Networking, Vol 11/2003, ISSN: 10636692, Is-sue1, DOI: 10.1109/TNET.2002.808407,
[2] STOICA, I., MORRIS, R., KARGER, D., KAASHOEK, M. F., AND BALAKRISHNAN, H. Chord: A scalable peer-to-peer lookup service for internet applications. Tech. Rep. TR-819, MIT LCS, March 2001. http://www.pdos.lcs.mit.edu/chord/papers/.
