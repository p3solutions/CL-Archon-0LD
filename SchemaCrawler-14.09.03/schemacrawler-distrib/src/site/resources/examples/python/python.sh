java -classpath $(echo ../../_schemacrawler/lib/*.jar | tr ' ' ':') schemacrawler.Main -server=hsqldb -database=schemacrawler -user=sa -password= -infolevel=standard -command script -infolevel=maximum -sorttables=false -outputformat $1