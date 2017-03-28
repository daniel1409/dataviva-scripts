time mysql -h<host> -u<user> -p<password> <database> 
--batch -e "select cnes, codmun, tp_unid, tp_leito, codleito, qt_exist, qt_contr, qt_sus, qt_nsus, competen1, regsaude from LEITOS_2008_STEP3" 
| sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > cnes_bed_2008.csv