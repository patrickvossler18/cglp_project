import helpers
import MySQLdb as mysql
import MySQLdb.cursors
import csv

db = mysql.connect(host='127.0.0.1', port=3306, user='root', db='cglp', passwd='cglp')
c = db.cursor(MySQLdb.cursors.DictCursor)

COUNTRY_LIST = ['Australia', 'Austria', 'Botswana', 'Belgium', 'Canada',
                    'Chile', 'Colombia', 'France', 'Germany', 'India',
                    'Ireland', 'Lesotho', 'Malawi', 'Malaysia', 'New Zealand',
                    'Nigeria', 'Papua New Guinea', 'Peru',
                    'Philippines', 'South Africa', 'Spain', 'Switzerland',
                    'Uganda', 'UK', 'USA', 'Zimbabwe']

country_files_total = {}
country_files_by_year = {}
num_files_total = 0
for country in COUNTRY_LIST:
    num_files = 0
    files = helpers.getCountryFiles('/home/ec2-user/CGLP_Data', country)
    years = {}
    for key, value in files.items():
        num_files += len(value)
        num_files_total += len(value)
        years[key] = len(value)
    country_files_by_year[country] = years
    country_files_total[country] = num_files

c.execute("select cd.country_name, count(*) from case_info ci join country_ids cd on ci.country_id = cd.country_id group by cd.country_name")
table_results_total = c.fetchall()
c.execute("select cd.country_name, ci.year, count(*) from case_info ci join country_ids cd on ci.country_id = cd.country_id group by cd.country_name,ci.year")
table_results_by_year = c.fetchall()


with open('/tmp/files_total.csv', 'wb') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in country_files_total.items():
        years = []
        for year in value:
            years.append(year)
        writer.writerow([key, years])

with open('/tmp/files_total_by_year.csv', 'wb') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in country_files_by_year.items():
        for year, amount in value.items():
            writer.writerow([key, year, amount])

with open('/tmp/table_results_total.csv', 'wb') as csv_file:
    writer = csv.writer(csv_file)
    for row in table_results_total:
        writer.writerow(row.values())

with open('/tmp/table_results_by_year.csv', 'wb') as csv_file:
    writer = csv.writer(csv_file)
    for row in table_results_by_year:
        writer.writerow(row.values())