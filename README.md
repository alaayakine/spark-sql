# Rapport Global des Applications Spark

## Application 3 : App2

### Introduction
L'application Spark "App2" traite des incidents stockés dans un fichier CSV. Elle utilise les API DataFrame et Dataset d'Apache Spark pour effectuer des analyses sur les incidents.

### Aperçu du Code
Le code Java configure une session Spark, lit les données depuis un fichier CSV ("ints.csv"), et réalise des opérations d'agrégation, notamment le calcul du nombre d'incidents par service et l'affichage des deux années avec le plus grand nombre d'incidents.

  ![image](https://github.com/alaayakine/spark-sql/assets/106708512/b4ab8d60-8a14-4036-b7dc-f9906d12a525)

### Requêtes et Résultats
1. **Nombre d'Incidents par Service**
   - Calcul et affichage du nombre d'incidents par service.

     
     ![image](https://github.com/alaayakine/spark-sql/assets/106708512/ac5b49ab-139f-4f41-8022-49a3983102ce)


2. **Deux Années avec le Plus Grand Nombre d'Incidents**
   - Calcul du nombre d'incidents par année.
   - Affichage des deux années avec le plus grand nombre d'incidents.
  
     
     ![image](https://github.com/alaayakine/spark-sql/assets/106708512/e154a3af-bebf-41b1-b6de-c5f77c6c76bf)

## Application 2 : App1

### Introduction
L'application Spark "App1" suit le même objectif de traitement de données hospitalières stockées dans une base de données MySQL.

### Aperçu du Code
Le code Java configure une session Spark, lit les données depuis les tables MySQL ("consultations", "medecins", "patients"), et réalise des opérations de traitement similaires à l'Application 1.

  ![image](https://github.com/alaayakine/spark-sql/assets/106708512/fa297fe8-f769-4d50-9817-f22821d72fa9)
  ![image](https://github.com/alaayakine/spark-sql/assets/106708512/bca305bf-c633-4f4b-b941-9ce536102744) ![image](https://github.com/alaayakine/spark-sql/assets/106708512/8aabd377-d3f9-4944-ae9a-fd7f0b1fe23d) ![image](https://github.com/alaayakine/spark-sql/assets/106708512/4de7276f-d9e1-4cf6-a47d-310da90afcab)




### Requêtes et Résultats
1. **Nombre de Consultations par Jour**
   - Calcul et affichage du nombre de consultations par jour.

     ![image](https://github.com/alaayakine/spark-sql/assets/106708512/7e34793e-f410-49d3-afa0-ec1f0fac5df5)

2. **Nombre de Consultations par Médecin**
   - Jointure entre les tables "consultations" et "medecins".
   - Regroupement par nom et prénom du médecin, avec comptage des consultations.

     ![image](https://github.com/alaayakine/spark-sql/assets/106708512/b589ba82-76b4-4195-864b-0c934eca8f6a)


3. **Nombre de Patients par Médecin**
   - Extension de la jointure pour inclure la table "patients".
   - Calcul et affichage du nombre de patients par médecin.

      ![image](https://github.com/alaayakine/spark-sql/assets/106708512/b3ead3d1-f587-475f-8249-d3b256d0dd2e)


### Conclusion
Les applications Spark ont réussi à traiter efficacement les données, fournissant des informations cruciales pour la gestion des consultations, la performance des médecins, et l'analyse des incidents. Les résultats obtenus peuvent contribuer à la prise de décisions stratégiques pour améliorer la qualité des soins et la gestion des incidents.

