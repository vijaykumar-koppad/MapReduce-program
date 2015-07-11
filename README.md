# Sentence Probability

This project involved calculation of the prbobility of each sentence in a huge corpus using MapReduce program on hadoop.
    
   Probability of the sentence of a word that exists at position i by using the formula below:
   
					P(i, w) = Num(i, w) / N;
                
   where Num(i, w) is the number of times that w exists at i-th position of a sentence, N is the total number of sentences with at least i words.
        
  The     probability     of      a       sentence        is      the     product of      probability     of      all its individual words. 
  
	1  2  3      4    5    6    7        [positions]
    It is a Project to learn mapreduce [words] 
    
    P(s) =  P(1 ,'It') P(2, 'is') P(3, 'a') P(4, 'project') P(5,'to') P(6,'learn') P(7,mapreduce).
    
   This program, 
   
  1. accepts two parameters to execute. One is for file path for the corpus, the other is where to place your result. 
  2. generates the top three sentences with the largest existence probability in the corpus.