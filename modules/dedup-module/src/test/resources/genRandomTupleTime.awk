function getRandom(n) { return 1 + int(rand() * n) }

BEGIN{
FS="|"
OFS="|"
SEPARATOR="|";
UNIQUE="UNIQUE";
DUPLICATE="DUPLICATE"
EXPIRED="EXPIRED"
dupCount=1;
dupEmitCount=1;
uniqueEmitCount=1;
expEmitCount=1;
currentCount=10000;
HUNDRED=100;
duplicate_percentage=20;
expiry_percentage=10;
expiry_limit=3600;
time_field=4;
srand();
}

{
    random_duplicate = getRandom(HUNDRED);
    timeAdd++;
    current_delay=timeAdd;
    cmd="date -d \"$(date)+"current_delay"sec\" \"+%Y-%m-%d %H:%M:%S\"";cmd | getline datum; close(cmd);
    $time_field=datum;
    latest_time=datum;
    $0 = $0 SEPARATOR UNIQUE SEPARATOR currentCount SEPARATOR uniqueEmitCount++ SEPARATOR (currentCount);
    print $0;
    currentCount++

    if( random_duplicate < duplicate_percentage + expiry_percentage)
    {
        dup_exp_list[dupCount++] = $0;
    }

    random_emmit = getRandom(HUNDRED);

    if( random_emmit < duplicate_percentage + expiry_percentage)
    {
      dup_random=getRandom(dupCount);
      $0= dup_exp_list[dup_random];
      if($0 ==""){
          next;
      }
      $(NF-2)=currentCount++;
            tuple_time=$time_field
      #print "latest_time is : " latest_time;
      #print "tuple_time is  : " tuple_time ;
            cmd="date -d \"$(date -d \""tuple_time"\")+"expiry_limit"sec\" \"+%Y-%m-%d %T\"";cmd | getline datum; close(cmd);
      tuple_exp_time=datum;
            cmd="date -d \"$(date -d \""latest_time"\")+"0"sec\" \"+%Y-%m-%d %T\"";cmd | getline datum; close(cmd);
      latest_time=datum;
      if(tuple_exp_time <= latest_time ){
          $(NF-1)=expEmitCount++;
          $(NF-3)=EXPIRED;
      }else{
          $(NF-1)=dupEmitCount++;
          $(NF-3)=DUPLICATE;
      }
      print $0;
    }
}
