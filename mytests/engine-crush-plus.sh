for arg1 in {10..13}
# host_weight_power
do
    for arg2 in {3..8}
    # fill_engine_hash_func_num
    do
        for arg3 in {10..13}
        # pg_weight_power
        do
            for arg4 in {3..8}
            # write_obj_hash_func_num
            do
                echo "Running: python engine-crush-plus.py $arg1 $arg2 $arg3 $arg4"
                python engine-crush-plus.py $arg1 $arg2 $arg3 $arg4
            done
        done
    done
done