time mysql -h<host> -u<user> -p<password> <database> --batch -e 
"select cnes, codmun, niv_dep, tp_unid, tipequip, codequip, qt_exist, qt_uso, ind_sus, competen1, regsaude from EQUI_2008_STEP3" 
| sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > cnes_equipment_2008.csv