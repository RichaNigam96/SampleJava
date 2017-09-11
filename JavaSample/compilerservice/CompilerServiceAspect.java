package com.amazon.dw.grasshopper.compilerservice;

import java.io.IOException;

import javax.measure.unit.Unit;

import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.jdom.JDOMException;
import com.amazon.coral.metrics.Metrics;
import com.amazon.dw.grasshopper.CompilerServiceException;
import org.json.JSONException;

@Aspect
public class CompilerServiceAspect {
    
    public static final double METRIC_INCREMENT_VALUE = 1.0;
    private static Logger LOGGER = Logger.getLogger(CompilerServiceAspect.class);
    
    
    /**
     * Logs and wraps exceptions from public methods in CompilerService
     * @param joinPoint
     * @return
     * @throws Throwable
     */
    @Around("execution(public * com.amazon.dw.grasshopper.compilerservice.CompilerService.*(..))")
        public Object around(final ProceedingJoinPoint joinPoint) throws Throwable{
            try{
                Object retVal =  joinPoint.proceed();
                return retVal;
            }
            catch(CompilerServiceException e){
                LOGGER.error(e.getMessage(), e);
                throw e;
            }
            catch(JSONException e) {
                LOGGER.error(e.getMessage(), e);
                throw new CompilerServiceException("Improperly formatted JSON in input: " + e, e);
            }
            catch(IOException e){
                LOGGER.error(e.getMessage(), e);
                throw new CompilerServiceException("General IOException:"+e.toString(),e);
            }
            catch(JDOMException e){
                LOGGER.error(e.getMessage(), e);
                throw new CompilerServiceException("General JDOMException:"+e.toString(),e);
            }
            catch(Throwable e){
                LOGGER.error(e.getMessage(), e);
                throw new CompilerServiceException("Internal exception: " + e.getMessage(), e);
            }
        }
     
    @Around("@annotation(compileAspectAnnotation)")
    public Object around(final ProceedingJoinPoint joinPoint, final CompileAspectAnnotation compileAspectAnnotation) throws Throwable{
        Metrics metrics = null;
        try{
            Object obj = joinPoint.getTarget();
            
            if(obj != null && CompilerService.class.isInstance(obj) )
                metrics = ((CompilerService)obj).getMetrics();
            else
                LOGGER.warn("Can't write metrics - Object is " +obj.getClass().toString());
            
            if(metrics!=null)
                metrics.addCount(compileAspectAnnotation.attemptMetricName(), METRIC_INCREMENT_VALUE  , Unit.ONE);
            Object retVal =  joinPoint.proceed();
            return retVal;
        }
        catch(Throwable e){
            if(metrics!=null)
                metrics.addCount(compileAspectAnnotation.failureMetricName(), METRIC_INCREMENT_VALUE  , Unit.ONE);
            throw e;
        }
    }
}
