time mysql -h<host> -u<user> -p<password> <database> 
--batch -e "select cnes, tp_unid, cbo, cns_prof, vinculac, prof_sus, horaoutr, horahosp, hora_amb, competen1, regsaude from PROF_2008_STEP3" | 
sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > cnes_professional_2008.csv &&