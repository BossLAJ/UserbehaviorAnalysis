var array = Array(32,12,45,23,89,67)

val result = for(i <- 0 to array.length-1 )yield {
	if(i % 2 == 0){
		var temp = array(i)
		array(i) = array(i+1)
		array(i+1) = temp 
	}
	array(i)
}
println(result.mkString(","))