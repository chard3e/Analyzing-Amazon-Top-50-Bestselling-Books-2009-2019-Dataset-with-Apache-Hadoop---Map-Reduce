First, dataset was converted to TSV format and was made compatible to Java with the Python script below:

![image](https://github.com/user-attachments/assets/4636dea4-40a9-4fe7-aca9-5da825b6f814)


The code was compiled with the "javac -cp "$(hadoop classpath)" Hw1.java" command and jar file was made with "jar cf Hw1.jar *.class" command.

1. Calculating and listing the total number of of reviews for each book genre
  hadoop jar Hw1.jar Hw1 total-reviews /project/amazon_bestsellers_cleaned.tsv
output_total_reviews
  ![image](https://github.com/user-attachments/assets/e13aa59c-8a22-4bb2-b4ac-2d5cecc81ee2)
  hdfs dfs -cat output_total_reviews/part-r-00000
  ![image](https://github.com/user-attachments/assets/433052d4-5fe2-4e43-873f-27601bf0f4e2)
2. Calculating and listing the average price of books based on their release year
  hadoop jar Hw1.jar Hw1 average-price /project/amazon_bestsellers_cleaned.tsv
output_average_price
  ![image](https://github.com/user-attachments/assets/b34efd07-f3b7-42ee-a565-1c1e6172a253)
  hdfs dfs -cat output_average_price/part-r-00000

  ![image](https://github.com/user-attachments/assets/7bcbe314-65b7-403c-b4eb-66dffa0b2c93)
  
3. Calculating and listing the average user rating for each book genre
  hadoop jar Hw1.jar Hw1 user-rating /project/amazon_bestsellers_cleaned.tsv
output_user_rating
  ![image](https://github.com/user-attachments/assets/47b85abd-03bf-4b02-b38c-dda05689370e)
  hdfs dfs -cat output_user_rating/part-r-00000
  ![image](https://github.com/user-attachments/assets/aefe490e-ebbd-4f62-ae38-471edcfce19a)

4. Listing the top 3 most reviewed authors for each genre
  hadoop jar Hw1.jar Hw1 popular-authors /project/amazon_bestsellers_cleaned.tsv
output_authors
  ![image](https://github.com/user-attachments/assets/d604fdf9-181b-44ec-a814-0c03edc4d0ae)
  hdfs dfs -cat output_authors/part-r-00000
  ![image](https://github.com/user-attachments/assets/ef062d69-c1ef-4455-a9e5-89651df3cadc)

6. After grouping the books into three time periods, calculating the average number of reviews by genre
  hadoop jar Hw1.jar Hw1 year-partition /project/amazon_bestsellers_cleaned.tsv
output_year_partition
  ![image](https://github.com/user-attachments/assets/1b265fdc-0760-47bf-9e07-8723eddbdb2b)
  hdfs dfs -cat output_year_partition/part-r-00000
  ![image](https://github.com/user-attachments/assets/043d4018-9af1-417b-877d-55744438c1cb)

6. For each of the three year ratings; finding the most reviewed fiction book that meets the following criteria:
-Rating above 4.30 and price over $20
-If no exact match is found, listing the closest matching book instead
  hadoop jar Hw1.jar Hw1 conditional-split /project/amazon_bestsellers_cleaned.tsv
output_conditional_split
  ![image](https://github.com/user-attachments/assets/e4f0ba0d-d969-4069-8a6c-deb79cf4e039)
  hdfs dfs -cat output_conditional_split/part-r-00000
  ![image](https://github.com/user-attachments/assets/d4409bc8-336e-4d07-b343-0dd12c29e48a)





