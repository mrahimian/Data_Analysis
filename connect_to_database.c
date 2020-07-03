#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <math.h>
#include <locale.h>
//setlocale(LC_ALL, "en_US.UTF-8");
void do_exit(PGconn *conn, PGresult *res) {
    
    fprintf(stderr, "%s\n", PQerrorMessage(conn));    

    PQclear(res);
    PQfinish(conn);    
    
    exit(1);
}

//put data in fp_city_aggregation
void city_store_sum(char* row[100], PGconn* conn){
	char ommit[100]={};
	for(int j=0;row[7][j]!='\n';j++){
                ommit[j]=row[7][j];
        }
	char insert[700];
        sprintf(insert,"INSERT INTO fp_city_aggregation VALUES('%s','%s',%s,%s,%s)ON CONFLICT (time,city) do update set quantity = (select quantity from fp_city_aggregation where time = '%s' and city = '%s')+%s , has_sold = (select has_sold from fp_city_aggregation where time = '%s' and city = '%s')+%s , price = (select price from fp_city_aggregation where time = '%s' and city = '%s')+%s;",row[0],row[2],row[5],row[6],ommit,row[0],row[2],row[6],row[0],row[2],ommit,row[0],row[2],row[5]);
	char insert1[1000];
        sprintf(insert1,"INSERT INTO fp_store_aggregation VALUES('%s','%s',%s,%s,%s,%s)ON CONFLICT (time,city,market_id) do update set quantity = (select quantity from fp_store_aggregation where time = '%s' and city = '%s' and market_id = %s)+%s , has_sold = (select has_sold from fp_store_aggregation where time = '%s' and city = '%s' and market_id = %s)+%s , price = (select price from fp_store_aggregation where time = '%s' and city = '%s' and market_id = %s)+%s;",row[0],row[2],row[3],row[5],row[6],ommit,row[0],row[2],row[3],row[6],row[0],row[2],row[3],ommit,row[0],row[2],row[3],row[5]);
	
	PGresult *res = PQexec(conn , insert);       
       	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        do_exit(conn, res);
        }
        PQclear(res);

	PGresult *res2 = PQexec(conn , insert1);
        if (PQresultStatus(res2) != PGRES_COMMAND_OK) {
        do_exit(conn, res2);
        }
        PQclear(res2);

}

//put data in fp_stores_data TABLE 
void send_to_DB(char* row[100], PGconn* conn)
{
	int i;
	char insert[300];
	char ommit[100]={};
	for(int j=0;row[7][j]!='\n';j++){
                ommit[j]=row[7][j];
        }
	sprintf(insert,"INSERT INTO fp_stores_data VALUES(%s,'%s','%s',%s,%s,%s,%s,%s)",row[0],row[1],row[2],row[3],row[4],row[5],row[6],ommit);
	PGresult *res = PQexec(conn , insert);
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
       	do_exit(conn, res);
    	}
    	PQclear(res);
}

//get data from files and separate each line as an array of strings(attributes) and pass to send_to_DB method
//wouldn't send lines have more or less data than 8
//wouldn't send lines which the int atts are filled with string
void separate_data(char file_name[256],PGconn* conn )
{
FILE* fh;
char str[50] = "/tmp/final_project/";
strcat(str,file_name);
fh = fopen(str , "r");

//check if file exists
if (fh == NULL){
    printf("file does not exists ");
    return ;
}


//read line by line
const size_t line_size = 8192;
int i=0;


int bufferLength = 4096;
char buffer[bufferLength];


char* split[4096];
char delim[] = ",";
int j=0;
while(fgets(buffer, bufferLength, fh)) {
    char *ptr = strtok(buffer, delim);
                int k=0;
                int flag=0;
                int qout = 0;
                while(ptr != NULL)
                {       
                        split[k]=ptr;
                        if(atoi(ptr)==0 && ((int)sizeof(ptr))!=sizeof(int) && (k>=3 && k<=7) && j!=0){
                        flag = 1;
                        break;
                        }
                        ptr = strtok(NULL, delim);
                        k++;
                }
                if(k != 8 || flag==1){
                   continue;
                           }
                send_to_DB(split,conn);
                city_store_sum(split,conn);
                int p ;
                for(p=0;p<k;p++){
                  //      printf("%s ", split[p]);
                }
		//printf("\n");
		j++;

}

fclose(fh);
}

//search "/tmp/final_project" directory and send each file to separate_data method
void get_data(const char *path, PGconn* conn)
{
    struct dirent *dp;
    DIR *dir = opendir(path);
    
    // Unable to open directory stream
    if (!dir)
        return;

    while ((dp = readdir(dir)) != NULL)
    {
	if(strcmp(".",dp->d_name)!=0 && strcmp("..",dp->d_name)!=0){
	separate_data(dp->d_name,conn);	
	//delete current file
	char del[300];
        sprintf(del,"rm /tmp/final_project/%s",dp->d_name);
        system(del);
	}
	//printf("%s\n",dp->d_name);
    }
    
    // Close directory stream
    closedir(dir);
}

int main() { 
    //connect to database
    system("~/final_project/download.sh");
    PGconn *conn = PQconnectdb("user=mohammadreza dbname=fpdb");
    if (PQstatus(conn) == CONNECTION_BAD) {
    	 fprintf(stderr, "Connection to database failed: %s\n",
            PQerrorMessage(conn));        
       	 PQfinish(conn);
       	 exit(1);   
    }
   get_data("/tmp/final_project",conn);
   //printf("%s",add_qout("hello"));
   //close connection
   PQfinish(conn);

    return 0;
}

