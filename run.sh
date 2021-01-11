cat ml-1m/ratings.dat | awk -F '::' '{if($4<=975000000 && $3 >= 4)print $1,$2}' > ml-1m/ratings_train.dat
cat ml-1m/ratings.dat | awk -F '::' '{if($4>975000000 && $3 >= 4)print $1,$2}' > ml-1m/ratings_test.dat
