#' A remOutliers Function
#'
#' This function allows you to find outliers and remove them.
#' @param x A numerical vector
#' @param r A numerical number
#' @keywords outliers
#' @export list
#' @examples
#' out=remOutliers(x=c(rnorm(n=100),-111,222),r=5)
#' 
x <- c(20,   20,   20,   20,   10,   10,   10,   10,   30,   30)

x

remOutliers <-function(x,r=5.0){  
  Q13=quantile(x, probs = c(0.25,0.75), na.rm = T)
  Q1=Q13[1];  Q3=Q13[2]
  IQR=Q3-Q1
  majorO1=Q1-r*IQR; majorO2= Q3+r*IQR
  B=x<majorO1 | x>majorO2; 
  xValid=x;oInd=vector('integer',0)
  if(any(B,na.rm =T)){
    oInd=which(B); 
    xValid=x[-oInd];
  }    
  return(list(xValid=xValid,oInd=oInd))
}