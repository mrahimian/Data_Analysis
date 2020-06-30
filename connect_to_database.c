#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>

void do_exit(PGconn *conn, PGresult *res) {
    
    fprintf(stderr, "%s\n", PQerrorMessage(conn));    

    PQclear(res);
    PQfinish(conn);    
    
    exit(1);
}

void send_to_DB(char* row[100], PGconn* conn)
{
	int i;
	char ommit[10];
	char insert[100] = "INSERT INTO fp_stores_data VALUES (";
	for(i=0;i<7;i++){
		strcat(insert,row[i]);
		strcat(insert,",");
	}
	for(int j=0;row[7][j]!='\n';j++){
		ommit[j]=row[7][j];
	}
	strcat(insert,ommit);
	strcat(insert,");");
	printf("%s\n",insert);
	PGresult *res = PQexec(conn , insert);
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
       	do_exit(conn, res);
    	}
    	PQclear(res);
}

void separate_data(char file_name[50],PGconn* conn )
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
const size_t line_size = 300;
char line_arr[10][100];
int i=0;

while (fgets(line_arr[i], line_size, fh) != NULL)  {
   i++;
}

fclose(fh);
char* split[100];
char delim[] = ", ";
int j;
for(j=1;j<i;j++){
        char *ptr = strtok(line_arr[j], delim);
                int k=0;
                int flag=0;
                while(ptr != NULL)
                {
                        split[k] = ptr;
                        if(atoi(ptr)==0 && ((int)sizeof(ptr))!=sizeof(int) && (k>=3 && k<=7) && j!=0){
                                printf("%s : %d--%ld--%d\n",ptr,atoi(ptr),sizeof(ptr),k);
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
		int p ;
                for(p=0;p<k;p++){
                        printf("%s\n", split[p]);
                }

}
}

void get_data(const char *path, PGconn* conn)
{
    struct dirent *dp;
    DIR *dir = opendir(path);
    
    // Unable to open directory stream
    if (!dir)
        return;

    while ((dp = readdir(dir)) != NULL)
    {
	//printf("hello");
	if(strcmp(".",dp->d_name)!=0 && strcmp("..",dp->d_name)!=0){
	separate_data(dp->d_name,conn);	
	}
    }
    
    // Close directory stream
    closedir(dir);
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
   get_data("/tmp/final_project",conn);
   //PGresult *res = PQexec(conn , "update fp_stores_data set quantity = 10+5 where city = 'tehran';");
   PQfinish(conn);

    return 0;
}

