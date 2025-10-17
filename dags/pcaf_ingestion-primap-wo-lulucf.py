import requests
from airflow import DAG
import pendulum
from airflow.decorators import task
import os.path
import urllib.request

import os
import urllib.request
import zipfile
import pandas as pd
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.trino.hooks.trino import TrinoHook



with DAG(
    "pcaf_ingestion-primap-wo-lulucf", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily", catchup=False
) as dag:

    @task(
        task_id="load_data_to_s3_bucket"
    )
    def load_data_to_s3_bucket():
        import pandas as pd
        import zipfile
        import urllib.request
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        url = "https://zenodo.org/records/13752654/files/Guetschow_et_al_2024a-PRIMAP-hist_v2.6_final_13-Sep-2024.csv?download=1"
        local_file = "Guetschow_et_al_2024a-PRIMAP-hist_v2.6_final_13-Sep-2024.csv"
        if os.path.isfile(local_file):
             os.remove(local_file)

        if not os.path.isfile(local_file):
            with urllib.request.urlopen(url) as file:
                with open(local_file, "wb") as new_file:
                    new_file.write(file.read())
                new_file.close()

        s3_hook = S3Hook(aws_conn_id='s3')

        with open(local_file,  "r") as file_descriptor:
            df = pd.read_csv(file_descriptor)
            parquet_bytes = df.to_parquet(compression='gzip')
            s3_hook.load_bytes(parquet_bytes, bucket_name= "pcaf", key="raw/primap_wo_lulucf/primap_wo_lulucf.parquet", replace=True)
        
    trino_create_schema = SQLExecuteQueryOperator(
        task_id="trino_create_schema",
        conn_id="trino_connection",
        sql=f"CREATE SCHEMA IF NOT EXISTS hive.pcaf WITH (location = 's3a://pcaf/data')",
        handler=list,
    )

    trino_drop_table = SQLExecuteQueryOperator(
        task_id="trino_drop_table",
        conn_id="trino_connection",
        sql=f"drop table if exists hive.pcaf.primap_wo_lulucf",
        handler=list,
    )

    trino_create_primap_table = SQLExecuteQueryOperator(
        task_id="trino_create_primap_table",
        conn_id="trino_connection",
        sql=f"""create table if not exists hive.pcaf.primap_wo_lulucf (
                        "source" varchar,"scenario (PRIMAP-hist)" varchar,"area (ISO3)" varchar,"entity" varchar,"unit" varchar,"category (IPCC2006_PRIMAP)" varchar,"1750" double,"1751" double,"1752" double,"1753" double,"1754" double,"1755" double,"1756" double,"1757" double,"1758" double,"1759" double,"1760" double,"1761" double,"1762" double,"1763" double,"1764" double,"1765" double,"1766" double,"1767" double,"1768" double,"1769" double,"1770" double,"1771" double,"1772" double,"1773" double,"1774" double,"1775" double,"1776" double,"1777" double,"1778" double,"1779" double,"1780" double,"1781" double,"1782" double,"1783" double,"1784" double,"1785" double,"1786" double,"1787" double,"1788" double,"1789" double,"1790" double,"1791" double,"1792" double,"1793" double,"1794" double,"1795" double,"1796" double,"1797" double,"1798" double,"1799" double,"1800" double,"1801" double,"1802" double,"1803" double,"1804" double,"1805" double,"1806" double,"1807" double,"1808" double,"1809" double,"1810" double,"1811" double,"1812" double,"1813" double,"1814" double,"1815" double,"1816" double,"1817" double,"1818" double,"1819" double,"1820" double,"1821" double,"1822" double,"1823" double,"1824" double,"1825" double,"1826" double,"1827" double,"1828" double,"1829" double,"1830" double,"1831" double,"1832" double,"1833" double,"1834" double,"1835" double,"1836" double,"1837" double,"1838" double,"1839" double,"1840" double,"1841" double,"1842" double,"1843" double,"1844" double,"1845" double,"1846" double,"1847" double,"1848" double,"1849" double,"1850" double,"1851" double,"1852" double,"1853" double,"1854" double,"1855" double,"1856" double,"1857" double,"1858" double,"1859" double,"1860" double,"1861" double,"1862" double,"1863" double,"1864" double,"1865" double,"1866" double,"1867" double,"1868" double,"1869" double,"1870" double,"1871" double,"1872" double,"1873" double,"1874" double,"1875" double,"1876" double,"1877" double,"1878" double,"1879" double,"1880" double,"1881" double,"1882" double,"1883" double,"1884" double,"1885" double,"1886" double,"1887" double,"1888" double,"1889" double,"1890" double,"1891" double,"1892" double,"1893" double,"1894" double,"1895" double,"1896" double,"1897" double,"1898" double,"1899" double,"1900" double,"1901" double,"1902" double,"1903" double,"1904" double,"1905" double,"1906" double,"1907" double,"1908" double,"1909" double,"1910" double,"1911" double,"1912" double,"1913" double,"1914" double,"1915" double,"1916" double,"1917" double,"1918" double,"1919" double,"1920" double,"1921" double,"1922" double,"1923" double,"1924" double,"1925" double,"1926" double,"1927" double,"1928" double,"1929" double,"1930" double,"1931" double,"1932" double,"1933" double,"1934" double,"1935" double,"1936" double,"1937" double,"1938" double,"1939" double,"1940" double,"1941" double,"1942" double,"1943" double,"1944" double,"1945" double,"1946" double,"1947" double,"1948" double,"1949" double,"1950" double,"1951" double,"1952" double,"1953" double,"1954" double,"1955" double,"1956" double,"1957" double,"1958" double,"1959" double,"1960" double,"1961" double,"1962" double,"1963" double,"1964" double,"1965" double,"1966" double,"1967" double,"1968" double,"1969" double,"1970" double,"1971" double,"1972" double,"1973" double,"1974" double,"1975" double,"1976" double,"1977" double,"1978" double,"1979" double,"1980" double,"1981" double,"1982" double,"1983" double,"1984" double,"1985" double,"1986" double,"1987" double,"1988" double,"1989" double,"1990" double,"1991" double,"1992" double,"1993" double,"1994" double,"1995" double,"1996" double,"1997" double,"1998" double,"1999" double,"2000" double,"2001" double,"2002" double,"2003" double,"2004" double,"2005" double,"2006" double,"2007" double,"2008" double,"2009" double,"2010" double,"2011" double,"2012" double,"2013" double,"2014" double,"2015" double,"2016" double,"2017" double,"2018" double,"2019" double,"2020" double,"2021" double, "2022" double,"2023" double
                        )
                        with (
                         external_location = 's3a://pcaf/raw/primap_wo_lulucf/',
                         format = 'PARQUET'
                        )""",
        handler=list,
        outlets=['hive.pcaf.primap_wo_lulucf']
    )

    load_data_to_s3_bucket()  >> trino_create_schema >> trino_drop_table >> trino_create_primap_table