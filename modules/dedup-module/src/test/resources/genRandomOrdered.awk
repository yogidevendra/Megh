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
expiry_limit=86400;
srand();
}

{
    random_duplicate = getRandom(HUNDRED);
    $0 = $0 SEPARATOR UNIQUE SEPARATOR currentCount SEPARATOR uniqueEmitCount++ SEPARATOR (currentCount);
    print $0;
    current_exp_key = currentCount++ ;

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
      if($NF <= current_exp_key - expiry_limit ){
          $(NF-1)=expEmitCount++;
          $(NF-3)=EXPIRED;
      }else{
          $(NF-1)=dupEmitCount++;
          $(NF-3)=DUPLICATE;
      }
      print $0;
    }
}
