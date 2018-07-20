val arrays=Array(1,2,3,4,5,6,7,8,9,10)
val s3=for(item<-arrays if item%2==0) yield item*2
s3
