#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>


void do_exit(PGconn *conn, PGresult *res) {
    
    fprintf(stderr, "%s\n", PQerrorMessage(conn));    

    PQclear(res);
    PQfinish(conn);    
    
    exit(1);
}

int main() {
    char * com ; 
    PGconn *conn = PQconnectdb("user=mohammadreza dbname=fpdb");

    if (PQstatus(conn) == CONNECTION_BAD) {
    	 fprintf(stderr, "Connection to database failed: %s\n",
            PQerrorMessage(conn));        
       	 PQfinish(conn);
       	 exit(1);   
    }
    com =  "COPY fp_stores_data(time,province,city,market_id,product_id,price,quantity,has_sold) FROM '/home/mohammadreza/final_project/sth.csv' DELIMITER ',' CSV HEADER;";
    PGresult *res = PQexec(conn , com);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        do_exit(conn, res);
    }
    
    PQclear(res);
    PQfinish(conn);

    return 0;
}
