**Elasticsearch REST DataAdapter**
This is an additional plugin based on ElasticJasper written by Fabio Torchetti (https://github.com/WedjaaOpen/ElasticJasper) that allows you to create reports with data from an ElasticSearch cluster.
The difference between two plugins is what Elasticsearch REST Plugin to query data uses REST API instead of Java API as ElasticJasper Plugin does it. 
The reason to have Elasticsearch REST Plugin is what Elasticsearch team keep changing Java API from version to version, but REST API almost remains the same. 
This approach potentially providing ability to use the same plugin for different Elasticsearch versions(2.x, 5.x, 6.x, 7.x).

**How To Compile**
The best way in case to develop, compile and test this plugin is to use Jaspersoft Studio with Ecplise. To do so:
1. Install Jaspersoft Studio Plugin for Ecplise.You could find a lot of examples how to install Jaspersoft Studio Plugin for Ecplise including one below: 
https://o7planning.org/en/10247/install-jaspersoft-studio-into-eclipse
2. Clone this project and open it in Eclipse.
3. Add libraries "JasperReports Library" and "JasperReports Library Dependencies" to your project. They should be predefined libraries after Jaspersoft Studio was installed.
4. Run your project as an "Eclipse Application".

**Support**
This software is released as is. We make no claim that it will do anything useful and it may potentially do harm to your self confidence. We will however keep an eye on the issues you open on GitHub and try an fix whatever it's broken.
