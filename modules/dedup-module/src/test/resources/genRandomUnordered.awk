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
expiry_limit=1;
unordered_expiry_key=5;
srand();
}

{
    random_duplicate = getRandom(HUNDRED);
    $0 = $0 SEPARATOR UNIQUE SEPARATOR currentCount SEPARATOR uniqueEmitCount++ SEPARATOR (currentCount);
    current_exp_key=$unordered_expiry_key;
    if(prev_exp_key != current_exp_key)
    {
        exp_key_list[prev_exp_key] = -1;
        exp_key_list[current_exp_key] = 1;
    }
    print $0;
    currentCount++
    prev_exp_key=$unordered_expiry_key;

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
      $(NF-2)=currentCount;
        
      if(exp_key_list[$unordered_expiry_key] < 0){
          $(NF-1)=expEmitCount++;
          $(NF-3)=EXPIRED;
      }else if(exp_key_list[$unordered_expiry_key] > 0){
          $(NF-1)=dupEmitCount++;
          $(NF-3)=DUPLICATE;
      }else{
          next;
      }
      print $0;
      currentCount++;
    }
}
