# Final Project Big Data Kelompok 2 Kelas B

| Nama                     | NRP        |
|--------------------------|------------|
| Atha Rahma               | 5027221030 |
| Jeany Aurellia Putri D   | 5027221008 |
| Clara Valentina          | 5027221016 |
| Angella Christie         | 5027221047 |
| Monika Damelia H         | 5027221011 |

## Alur Kerja
1. Pertama buat file ` docker-compose.yml ` yang berisi [code ini](kafka/docker-compose.yml).
2. Setelah itu jalankan command `sudo docker compose -f docker-compose.yml up -d` pada terminal anda.
3. Setelah itu jalankan kafka dengan menambahkan topic menggunakan command ```docker exec -it kafka kafka-topics.sh --create --topic property-sales --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1```.
4. Buka localhost MiniO `http://localhost:9090` dan masukkan username = `dua` dan password = `kelompok2`
5. Buat bucket baru bernama `property-data`
7. Jalankan [consumer.py](kafka/consumer.py) dan [produser.py](kafka/producer.py)
8. Setelah pada consumer terbentuk 1 batch, jalankan [prosesdata.py](data/prosesdata.py)
9. Pada prosesdata.py data akan dilakukan modeling prediksi harga dengan menggunakan random forest. Adapun kolom-kolom yang digunakan sebagai acuan adalah Rooms, Type, Distance, dan BuildingArea dengan Price sebagai kolom target.
10. Hasil dari modeling berupa file .pkl yang nantinya akan muncul setelah modeling selesai.

Output Producer :
![image](https://github.com/user-attachments/assets/e2db866a-d2b1-4e55-84c8-7da3a61869d3)


Output Consumer :
<img width="602" alt="image" src="https://github.com/user-attachments/assets/752bc0f9-3fe0-4c37-bdb8-2556b69ee30f" />


Tampilan bucket di miniO :
<img width="956" alt="image" src="https://github.com/user-attachments/assets/841b7ed1-a5c4-41dc-98c2-4374584a892d" />

Running modelling :
<img width="589" alt="image" src="https://github.com/user-attachments/assets/62ac85b3-7af8-4317-8e55-407dc1e013ef" />



