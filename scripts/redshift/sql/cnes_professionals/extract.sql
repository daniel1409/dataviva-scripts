time mysql -h<host> -u<user> -p<password> <database> 
--batch -e "select cnes, codmun, tp_unid, cbo, cns_prof, vinculac, prof_sus, horaoutr, horahosp, hora_amb, competen, regsaude, niv_hier_2 from PROF_2008_STEP3" |
sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > cnes_professional_2008.csv &&