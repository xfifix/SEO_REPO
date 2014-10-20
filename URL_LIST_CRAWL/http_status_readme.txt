J'ai créé une base pour faire plateforme de mise à jour des URLs dont le statut HTTP est à obtenir :
db.url=jdbc:postgresql://localhost/HTTPSTATUSDB
db.user=postgres
db.passwd=mogette

J'y ai créé la base suivante :
CREATE TABLE IF NOT EXISTS HTTPSTATUS_LIST (
    ID SERIAL PRIMARY KEY NOT NULL,
    URL TEXT,
    STATUS INT,
    DESCRIPTION TEXT,
    TO_FETCH BOOLEAN 
) TABLESPACE mydbspace;


C'est cette base qu'il faut remplir avec les URLs à chercher (l'id s'auto-incrémente).

Soit insérer des nouvelles

INSERT INTO HTTPSTATUS_LIST (URL,STATUS,DESCRIPTION,TO_FETCH)
    VALUES ('http://www.cdiscount.com/',-1,'Dummy',TRUE);
INSERT INTO HTTPSTATUS_LIST (URL,STATUS,DESCRIPTION,TO_FETCH)
    VALUES ('http://www.cdiscount.com/informatique/v-107-0.html',-1, 'Dummy',TRUE);
INSERT INTO HTTPSTATUS_LIST (URL,STATUS,DESCRIPTION,TO_FETCH)
    VALUES ('http://www.cdiscount.com/electrngomenager/v-110-0.html',-1,'Dummy',TRUE);


Soit updater des anciennes (d'où le champ description pour gérer des sujets)
UPDATE HTTPSTATUS_LIST SET TO_FETCH=true WHERE description= 'pagination_sitemap';
UPDATE HTTPSTATUS_LIST SET TO_FETCH=true WHERE URL like '%/v-%';



Lancer l'exécutable (copier le jar et le mettre là où vous voulez ):
cd /home/sduprey/My_Executable;
java -jar httpstatus_fetching.jar myuseragent mydescription nb_threads size_bucket

